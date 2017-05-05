# USAP-DC Web Submission and Dataset Search

# Objectives
+	To support the submission of data resources with appropriate documentation from investigators funded by the US Antarctic Program.
+	To support the acquisition of DataCite metadata needed for data publication
+	To support production and review of metadata suitable for discovery, evaluation and use of the data resource in the future
+	To support acquisition of DIF metadata needed for registration of USAP funded projects in the GCMD to fulfill  requirements of US investigators under the International Antarctic treaty.
	To support the basic keyword search capability of data resources hosted by the USAP-DC
+	To support the geographic search capability of data resources hosted by the USAP-DC

# Release History
+ 1.0.0 Initial public release from Ben Grange's code
+ 1.0.1 Updates to initial release made by Bob Arko

# What's here
This section provides a brief description of the source code files and folders in this repository:

## .gitignore
The .gitignore file for this project

## config.json
A JSON file containing any configuration information. Since this can include sensative information, the version here is blank, but can be used as a template.

## index.wsgi
Needed to run the production version of the web app on Apache.

## robots.txt
A file to give instructions about the site to web robots.

## start_usapdc
A bash startup script that will start up the USAP-DC Web App in development mode, using the correct python location and arguements.
To run:
> ./start_usapdc

## usap.py
All the python code for the web app, including the controller code for the templates.

## static/
Folder containing static files (css, fonts, images, javascripts) to be used by the web app.

## templates/
Folder containing the HTML templates (webpages).
