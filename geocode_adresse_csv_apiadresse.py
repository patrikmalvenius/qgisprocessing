# -*- coding: utf-8 -*-

"""
this scripts creates a processing util that accepts a CSV file of French addresses + other fields
and sends them over to the French national address API at https://api-adresse.data.gouv.fr. intended use is as processing step
in chain that associates a list of French addresses with their corresponding parcel IDs.
it creates 2 outputs, one with the geocoded results, including all return attributes from the API,
and one with the addresses that could not be geocoded. Both are vector geometry memory layers, one would prefer
that the one w/o geometries was type text but i haven't spent the time figuring out how
"""

from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (
    QgsField, 
    QgsFeature, 
    QgsFeatureSink, 
    QgsProcessing, 
    QgsProcessingAlgorithm,
    QgsProcessingParameterFeatureSink,
    QgsProcessingParameterFile,
    QgsProcessingParameterString,
    QgsFields,
    QgsPointXY,
    QgsGeometry,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsProcessingParameterField
    )

import requests
                       
class GeocodeAdresseAPI(QgsProcessingAlgorithm):
    INPUT = 'INPUT'
    OUTPUT = 'OUTPUT'
    BAD_OUTPUT = 'BAD_OUTPUT'
 
    def __init__(self):
        super().__init__()
 
    def name(self):
        return "GeocodeAdresseAPI"
     
    def tr(self, text):
        return QCoreApplication.translate("GeocodeAdresseAPI", text)
         
    def displayName(self):
        return self.tr("Geocode Adresses")
 
    def group(self):
        return self.tr("Geocode")
 
    def groupId(self):
        return "geocode"
 
    def shortHelpString(self):
        return self.tr("Geocode Adresses avec API national https://api-adresse.data.gouv.fr")
 
    def createInstance(self):
        return type(self)()
   
    def initAlgorithm(self, config=None):
  
        self.addParameter(QgsProcessingParameterFile(
            self.INPUT,
            self.tr("Input layer"),
            extension="csv",
            defaultValue=''))
        self.addParameter(QgsProcessingParameterFeatureSink(
            self.OUTPUT,
            self.tr("Résultat géocodé"),
            QgsProcessing.TypeVectorAnyGeometry))
        self.addParameter(QgsProcessingParameterFeatureSink(
            self.BAD_OUTPUT,
            self.tr("Résultat PAS géocodé"),
            QgsProcessing.TypeVectorAnyGeometry))
        self.addParameter(QgsProcessingParameterString(
            'separateur', 
            'Separateur dans fiche CSV', 
            defaultValue=';'))
        self.addParameter(QgsProcessingParameterField(
            'champadresse', 
            'Champ adresse', 
            type=QgsProcessingParameterField.String, 
            parentLayerParameterName=self.INPUT, 
            allowMultiple=False, 
            defaultValue=None))
        self.addParameter(QgsProcessingParameterField(
            'champcp', 
            'Champ code postal', 
            type=QgsProcessingParameterField.String, 
            parentLayerParameterName=self.INPUT, 
            allowMultiple=False, 
            defaultValue=None))
        self.addParameter(QgsProcessingParameterField(
            'champville', 
            'Champ ville', 
            type=QgsProcessingParameterField.String, 
            parentLayerParameterName=self.INPUT, 
            allowMultiple=False, 
            defaultValue=None))
 
    def processAlgorithm(self, parameters, context, feedback):
        #did try to use QgsBlockingNetworkRequest but could not understand how to put content of request in QtIODevice
        #that's why we're using request instead
        (sink, dest_id) = self.parameterAsSink(parameters, self.OUTPUT, context,
                                               fields, 1, QgsCoordinateReferenceSystem.fromEpsgId(2154))
        (sink2, dest_id2) = self.parameterAsSink(parameters, self.BAD_OUTPUT, context,
                                               fields, 1, QgsCoordinateReferenceSystem.fromEpsgId(2154))
        
        requestUrl = 'https://api-adresse.data.gouv.fr/search/csv/'
        payload = {
            'columns': [
            parameters['champadresse'],
            parameters['champcp'],
            parameters['champville'],
            ]
        }
        fields = QgsFields()
        sourceCrs = QgsCoordinateReferenceSystem.fromEpsgId(4326)
        destCrs = QgsCoordinateReferenceSystem.fromEpsgId(2154)
        tr = QgsCoordinateTransform(sourceCrs, destCrs, QgsProject.instance())   

        with open(parameters['INPUT'], mode='r', encoding='utf-8') as f:
            fileData = f.read()

        files = {'data': fileData}
        response = requests.post(requestUrl, data=payload, files=files)
        response.raise_for_status()
        responseArray =  response.text.splitlines()
        firstline = responseArray.pop(0)
        firstlineAsArray = firstline.split(parameters['separateur'])

        for x in firstlineAsArray:
            fields.append(QgsField(x, QVariant.String))

        while len(responseArray):
            tempFeature = QgsFeature(fields)
            tempFeature.setAttributes(responseArray.pop(0).split(parameters['separateur']))
            try:
                tempFeature.setGeometry(
                    QgsGeometry.fromPointXY(
                        tr.transform(
                            QgsPointXY(
                                float(tempFeature.attribute('longitude')),
                                float(tempFeature.attribute('latitude'))
                                )
                            )
                        )
                    )
                sink.addFeature(tempFeature, QgsFeatureSink.FastInsert)

            except ValueError:
                sink2.addFeature(tempFeature, QgsFeatureSink.FastInsert)

        return {self.OUTPUT: dest_id, self.BAD_OUTPUT: dest_id2}