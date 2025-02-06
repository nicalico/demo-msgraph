import os
import requests
import json
import logging
from datetime import timedelta, datetime, timezone
import asyncio
from pathlib import Path
import pandas as pd
from configparser import SectionProxy
from azure.identity import ClientSecretCredential
from azure.core.credentials import AccessToken
from msgraph import GraphServiceClient
from msgraph.generated.models.list_item import ListItem
from msgraph.generated.models.field_value_set import FieldValueSet
from msgraph.generated.sites.item.lists.item.items.items_request_builder import ItemsRequestBuilder
from kiota_abstractions.base_request_configuration import RequestConfiguration


class RateLimitedWorker:
    def __init__(self, n_requests, y_seconds):
        self.n_requests = n_requests
        self.y_seconds = y_seconds
        self.queue = asyncio.Queue()

    async def add_task(self, task_data):
        await self.queue.put(task_data)

    async def process_tasks(self, task_function):
        while not self.queue.empty():
            batch = []
            for _ in range(self.n_requests):
                try:
                    item = self.queue.get_nowait()
                    batch.append(item)
                    self.queue.task_done()  # Mark task as processed
                except asyncio.QueueEmpty:
                    break

            if batch:
                tasks = [task_function(item) for item in batch]
                await asyncio.gather(*tasks)

            if not self.queue.empty():
                await asyncio.sleep(self.y_seconds)

    async def run(self, task_function, items):
        for item in items:
            await self.add_task(item)
        await self.process_tasks(task_function)


class ExtractionChargement:
    settings: SectionProxy
    client_credential: ClientSecretCredential
    app_client: GraphServiceClient

    dir_brut = './data/brut/'
    dir_livrable = './data/livrable/'
    lignes_a_uploader = dir_brut + 'ajout.csv'
    lignes_a_retirer = dir_brut + 'suppression.csv'

    def __init__(self, config: SectionProxy):
        self.settings = config

        client_id = self.settings['clientId']
        tenant_id = self.settings['tenantId']
        client_secret = self.settings['clientSecret']
        self.site_id = self.settings['siteId']
        self.drive_id = self.settings['driveId']
        self.list_id = self.settings['listId']

        self.client_credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self.app_client = GraphServiceClient(self.client_credential)  # type: ignore

        self.token_file = './tokens.txt'

    def get_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as file:
                token_data = json.load(file)
                expires_at = datetime.fromisoformat(token_data['expires_at'])

                if datetime.now(timezone.utc) < expires_at:
                    return token_data['access_token']

        graph_scope = 'https://graph.microsoft.com/.default'
        token: AccessToken = self.client_credential.get_token(graph_scope)

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=token.expires_on - datetime.now(timezone.utc).timestamp())

        token_data = {
            'access_token': token.token,
            'expires_at': expires_at.isoformat(),
        }
        with open(self.token_file, 'w') as file:
            json.dump(token_data, file)

        return token.token

    async def obtenir_drive_id(self):
        site_id = self.site_id
        result = await self.app_client.sites.by_site_id(site_id).drives.get()
        print(result)

    def uploader_fichier(self):

        site_id = self.site_id
        drive_id = self.drive_id
        graph_scope = 'https://graph.microsoft.com/.default'
        access_token = (self.client_credential.get_token(graph_scope)).token
        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

        fichiers_a_uploader = [
            {'chemin': './data/livrable/', 'fichier': 'livrable.csv'}
        ]

        for fichier in fichiers_a_uploader:
            fichier_local = fichier['chemin'] + fichier['fichier']
            fichier_distant = fichier['fichier']
            url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/donnees/vitalisation/{fichier_distant}:/createUploadSession'

            response = requests.post(url, headers=headers, json={"item": {"@microsoft.graph.conflictBehavior": "replace"}})
            if response.status_code != 200:
                print(f"Erreur lors de la création de la session d'upload pour {fichier_distant}: {response.text}")
                continue

            upload_url = response.json().get('uploadUrl')
            if not upload_url:
                print(f"Échec de récupération de l'URL d'upload pour {fichier_distant}")
                continue

            chunk_size = 10 * 1024 * 1024  # 10 MB
            with open(fichier_local, 'rb') as file:
                file_size = file.seek(0, 2)  # Get file size
                file.seek(0)

                for start in range(0, file_size, chunk_size):
                    end = min(start + chunk_size - 1, file_size - 1)
                    file.seek(start)
                    chunk = file.read(chunk_size)
                    chunk_headers = {'Content-Range': f'bytes {start}-{end}/{file_size}'}

                    chunk_response = requests.put(upload_url, headers=chunk_headers, data=chunk)
                    if chunk_response.status_code not in [200, 201, 202]:
                        print(f"Erreur lors de l'upload de {fichier_distant} (chunk {start}-{end}): {chunk_response.text}")
                        break
                else:
                    print(f"Upload terminé pour {fichier_distant}")

    async def afficher_listes_disponibles(self):
        site_id = self.site_id
        result = await self.app_client.sites.by_site_id(site_id).lists.get()

        listes = {}

        for i in result.value:
            id_liste = i.additional_data['@odata.etag'].replace('"', '').split(',')[0]
            result = await self.app_client.sites.by_site_id(site_id).lists.by_list_id(id_liste).get()
            listes[result.display_name] = id_liste

        for l in listes:
            print(l, ': ', listes[l])

    async def mettre_a_jour_elagage_sharepoint(self):
        site_id = self.site_id
        list_id = self.list_id
        log_file = Path('data/logs/erreurs.log')
        log_file.parent.mkdir(exist_ok=True)  # Ensure the 'data' directory exists
        logging.basicConfig(
            filename=log_file,
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )

        async def creer_un_item(additional_data):
            request_body = ListItem(fields=FieldValueSet(additional_data=additional_data), )
            try:
                await self.app_client.sites.by_site_id(site_id).lists.by_list_id(list_id).items.post(request_body)
            except Exception as e:
                print(f"Error: {e}" + str(additional_data))
                logging.error(f"Error: {e} - Data: {additional_data}")

        # upload
        pd_a_uploader = pd.read_csv(self.lignes_a_uploader)
        dict_a_uploader = pd_a_uploader.to_dict(orient='records')

        create_worker = RateLimitedWorker(n_requests=200, y_seconds=10)  # https://learn.microsoft.com/en-us/graph/throttling-limits
        await create_worker.run(creer_un_item, dict_a_uploader)

        # suppression
        pd_a_retirer = pd.read_csv(self.lignes_a_retirer)
        liste_test_suppressions = pd_a_retirer['CodeBarre'].to_list()

        if len(pd_a_retirer) > 0:
            for x in liste_test_suppressions:
                query_params = ItemsRequestBuilder.ItemsRequestBuilderGetQueryParameters(
                    expand=['fields($select=Title)'],
                    filter="fields/Title eq '" + str(x) + "'"
                )

                request_configuration = RequestConfiguration(
                    query_parameters=query_params,
                )

                response = await self.app_client.sites.by_site_id(site_id).lists.by_list_id(list_id).items.get(request_configuration=request_configuration)

                if not response:
                    print(f"No items found with Title: {x}")
                    return

                tasks = [
                    self.app_client.sites.by_site_id(self.site_id).lists.by_list_id(list_id).items.by_list_item_id(item_id.id).delete()
                    for item_id in response.value
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for item_id, result in zip(response.value, results):
                    if isinstance(result, Exception):
                        print(f"Error deleting item with ID {item_id}: {result}")

        else:
            print('Rien à retirer')

    async def telecharger_delta_liste_sharepoint(self, listes):
        def log_date_extraction(timestamp=None):
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)  # Use current UTC time if no timestamp is provided
            timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            with open(log_extractions, "w") as f:
                f.write(timestamp.isoformat())

        def depuis():
            try:
                with open(log_extractions, "r") as f:
                    previous_time = datetime.fromisoformat(f.read().strip())
                current_time = datetime.now(timezone.utc)
                log_date_extraction(current_time)
                return previous_time

            except FileNotFoundError:
                current_time = datetime.now(timezone.utc)
                log_date_extraction(current_time)
                return current_time

        site_id = self.site_id
        log_extractions = './data/logs/derniere_extraction.log'
        fichier_output = './data/brut/extraction.csv'
        colonnes_output = ['col1', 'col2', 'col3']

        query_params = ItemsRequestBuilder.ItemsRequestBuilderGetQueryParameters(
            expand=["fields($select=" + ','.join(colonnes_output) + ")"],
            filter="fields/col3 eq 'terminé' and fields/Modified gt ' " + depuis().strftime("%Y-%m-%dT%H:%M:%S") + "'"
        )

        request_configuration = RequestConfiguration(
            query_parameters=query_params,
        )

        async def extraire_par_liste(liste):
            rows = []  # Use a list of dictionaries to collect rows

            methode = self.app_client.sites.by_site_id(site_id).lists.by_list_id(listes[liste])

            response = await methode.items.get(request_configuration=request_configuration)

            next_link = response.odata_next_link

            for i in response.value:
                row = i.fields.additional_data
                rows.append(row)  # Collect rows as dictionaries

            while next_link is not None:
                response = await methode.items.with_url(next_link).get()
                next_link = response.odata_next_link

                for i in response.value:
                    row = i.fields.additional_data
                    rows.append(row)

            pd_final = pd.DataFrame(rows)

            if pd_final.empty or pd_final.isna().all().all():
                pd_final = pd.DataFrame(columns=colonnes_output)  # Ensure correct structure

            return pd_final

        # Initialize the output file with headers
        pd.DataFrame(columns=colonnes_output).to_csv(fichier_output, mode='a', header=not os.path.exists(fichier_output), index=False, decimal=',')

        # Process each list and append results to the CSV file
        for liste in listes:
            pd_sortie = await extraire_par_liste(liste)
            pd_sortie.to_csv(fichier_output, mode='a', header=False, index=False, decimal=',')

    async def supprimer_un_item(self, id_sp):
        print("Départ: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        site_id = self.site_id
        list_id = self.list_id

        await self.app_client.sites.by_site_id(site_id).lists.by_list_id(list_id).items.by_list_item_id(id_sp).delete()

    async def supprimer_tous_les_items(self):
        site_id = self.site_id
        list_id = self.list_id

        methode = self.app_client.sites.by_site_id(site_id).lists.by_list_id(list_id).items
        methode_delete = self.app_client.sites.by_site_id(site_id).lists.by_list_id(list_id).items

        response = await methode.get()
        next_link = response.odata_next_link

        for v in response.value:
            await methode_delete.by_list_item_id(v.id).delete()

        while next_link:
            response = await methode.with_url(next_link).get()
            next_link = response.odata_next_link

            for v in response.value:
                await methode_delete.by_list_item_id(v.id).delete()

