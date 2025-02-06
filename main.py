import configparser
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from modules_graph import *

config = configparser.ConfigParser()
config.read(['config.cfg', 'config.dev.cfg'])
azure_settings = config['azure']

graph: ExtractionChargement = ExtractionChargement(azure_settings)


async def main():

    dict_operations = {
        1: {'titre': 'Extraction: afficher listes disponibles',
            'operation': lambda: listes_disponibles(graph),
            'async': True,
            'vis': False},
        2: {'titre': 'Extraction: décisions 3e tour',
            'operation': lambda: extraire_decisions(graph),
            'async': True,
            'vis': True},
        3: {'titre': 'Extraction: supprimer un item',
            'operation': lambda: supprimer_item(graph),
            'async': True,
            'vis': False},
        4: {'titre': 'Extraction: obtenir le drive-id',
            'operation': lambda: obtenir_drive_id(graph),
            'async': True,
            'vis': False},
        5: {'titre': 'Extraction: obtenir un token',
            'operation': lambda: ExtractionChargement.get_token(graph),
            'async': False,
            'vis': False},
        6: {'titre': 'Chargement Sharepoint: mettre à jour liste ÉLAGAGE',
             'operation': lambda: ExtractionChargement.mettre_a_jour_elagage_sharepoint(graph),
             'async': True,
             'vis': True},
        7: {'titre': 'Chargement Sharepoint: uploader les fichiers de TDB',
             'operation': lambda: ExtractionChargement.uploader_fichier(graph),
             'async': False,
             'vis': True},
        8: {'titre': 'Supprimer tous les items',
              'operation': lambda: ExtractionChargement.supprimer_tous_les_items(graph),
              'async': True,
              'vis': False},
        0: {'titre': 'Quitter',
            'operation': lambda: exit(),
            'async': False,
            'vis': True},
        }

    print('\nVeuillez choisir une opération:\n')
    for op in dict_operations:
        if dict_operations[op]['vis']:
            print('\t' + str(op) + '.' + (' ' * (4 - len(str(op)))) + dict_operations[op]['titre'])
    operation = int(input('\n> '))

    if operation not in dict_operations.keys():
        exit('Choix invalide')

    else:
        try:
            if dict_operations[operation]['async']:
                await dict_operations[operation]['operation']()

            else:
                dict_operations[operation]['operation']()
        except ODataError as odata_error:
            print('Error:')
            if odata_error.error:
                print(odata_error.error.code, odata_error.error.message)
            exit()


async def listes_disponibles(graph: ExtractionChargement):
    await graph.afficher_listes_disponibles()


async def obtenir_drive_id(graph: ExtractionChargement):
    await graph.obtenir_drive_id()


async def mettre_a_jour_elagage_sharepoint(graph: ExtractionChargement):
    await graph.mettre_a_jour_elagage_sharepoint()


async def supprimer_item(graph: ExtractionChargement):
    id_sp = input('Indiquez l\'ID à supprimer? ')
    await graph.supprimer_un_item(id_sp=id_sp)


async def extraire_decisions(graph: ExtractionChargement):
    listes = {}  # normalement, output d'une fonction qui identifie les listes sur lesquelles opérer
    await graph.telecharger_delta_liste_sharepoint(listes=listes)


async def supprimer_tous_les_items(graph: ExtractionChargement):
    await graph.supprimer_tous_les_items()


asyncio.run(main())
