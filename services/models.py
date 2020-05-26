from collections import OrderedDict
from flask_restplus import fields


#short model for the datasets
def getDatasetsShortModel(ns):
    return ns.model('Datasets - short', OrderedDict([
        ('dataset_uid', fields.String),
        ('title', fields.String),
        ('doi', fields.String),
        ('landing_page', fields.String)
    ]))

#short model for the projects
def getProjectsShortModel(ns):
    return ns.model('Projects - short', OrderedDict([
        ('proj_uid', fields.String),
        ('title', fields.String),
        ('landing_page', fields.String)
    ]))


#short model for Deployments
def getDeploymentsShortModel(ns):
    return ns.model('Deployments - short', OrderedDict([
        ('deployment_id', fields.String),
        ('deployment_type', fields.String)
]))