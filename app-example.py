import httplib2
import json
import pprint
from apiclient.discovery import build
from apiclient.errors import HttpError
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials
from oauth2client.client import AccessTokenRefreshError

# BigQuery API Settings
SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_NUMBER = 'acquired-vector-747' # REPLACE WITH YOUR Project ID

# Create a new API service for interacting with BigQuery
credentials = AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery_service = build('bigquery', 'v2', http=http)

class NothingToSee(webapp.RequestHandler):
   def get(self):
      self.response.out.write('oui.')

class ListDatasets(webapp.RequestHandler):
  def get(self):
     try:
       datasets = bigquery_service.datasets()
       listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
       self.response.out.write('Dataset list:')
       self.response.out.write(json.dumps(listReply))

     except HttpError as err:
       print 'Error in listDatasets:', pprint.pprint(err.content)

     except AccessTokenRefreshError:
       print ("Credentials have been revoked or expired, please re-run"
              "the application to re-authorize")

class ExportTable(webapp.RequestHandler):
   def get(self):
     projectId = "xxxxxxxxxxx"
     datasetId = "xxxxxxxxxxx"
     #query = "SELECT REGEXP_EXTRACT(user_agent, r'(1.0.\d+)') AS build_version, controller + ':' + action  AS controller_action, time FROM [" + projectId + ":" + datasetId + "." + tableId + "]" 
     query=("select us0.id AS user,tmpx.buildversion AS build, tmpx.day_of_time AS time, tmpx.controlleraction AS action FROM [" + projectId + ":" + datasetId + ".xxxxxxxxxxx] AS us0"
            " JOIN EACH "
            "(SELECT "
            "oa0.resource_owner_id AS resource_owner_id,"
            "STRFTIME_UTC_USEC"
            " (tmp.time,'%Y-%m-%d %H:%M') AS day_of_time,"
            "tmp.buildversion AS buildversion "
            "FROM "
            "(SELECT "
            "REGEXP_EXTRACT(lo0.Authorization, r'Bearer (\w+)') AS token,"
            "REGEXP_EXTRACT(lo0.user_agent, r'^Game SDK/(1.0.\d+)') AS buildversion,"
            "lo0.time AS time,(lo0.controller + ':' + lo0.action) AS controllerAction"
            " FROM [" + projectId + ":" + datasetId + ".xxxxxxxxxxx] AS lo0"
            ") AS tmp"
            " JOIN EACH [" + projectId + ":" + datasetId + ".xxxxxxxxxxx] AS oa0"
            " ON tmp.token = oa0.token"
            " GROUP BY resource_owner_id, buildversion,day_of_time, controlleraction"
            ") AS tmpx"
            " ON us0.id = tmpx.resource_owner_id"
            )


     url = "https://www.googleapis.com/bigquery/v2/projects/" + projectId + "/jobs"

     jobCollection = bigquery_service.jobs()
     jobData = {
         "projectId": projectId,
         "configuration": {
            "query": {
               "query": query,
               "destinationTable":{
                  "projectId": projectId,
                  "datasetId": datasetId,
                  "tableId": "xxxxxxxxxxx"
                  },
                  "writeDisposition": "WRITE_TRUNCATE"
            }
         }
      }
     print jobData
     insertJob = jobCollection.insert(projectId=projectId, body=jobData).execute()
     import time
     while True:
       status =jobCollection.get(projectId=projectId, jobId=insertJob['jobReference']['jobId']).execute()
       print status
       if 'DONE' == status['status']['state']:
         self.response.out.write("Done exporting!")
         return
       self.response.out.write('Waiting for export to complete..')
       time.sleep(10)

app = webapp.WSGIApplication(
    [('/listdatasets', ListDatasets),('/export', ExportTable), ('/', NothingToSee)],
    debug=True)

def main():
  run_wsgi_app(app)



