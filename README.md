# Stack Overflow Analysis
The website is a valuable resource for learning about programming languages, tools, and 
approaches because it has over 18 million questions and 29 million answers. In this essay, 
I look at how Stack Overflow data is used for analytics. I especially examine the data's capacity 
to illuminate developers' tastes and needs as well as fads and trends in programming languages. 
I detail the method we used to obtain and preprocess the data and give various examples of the 
experiments we conducted utilizing the dataset. My findings provide light on the discipline's 
current state and prospective future directions while demonstrating the relevance of Stack Overflow 
data for addressing software development-related research questions.

# Requirements
To have the necessary libraries run the following code:

pip install -r requirements.txt


Requirements and Flow
Please, perform the following steps to run the project in you local machine:

- 1) Please make sure to have Python installed on your machine
- 2) Please make sure to have all the files for this project in one directory
- 3) Please create a Python virtual environment as follows:
python -m venv env
-4) Please install all the necessary libraries run the following code:
pip install -r requirements.txt
-5) Please run the infrastructure_initiation.py file only once as follows:
python infrastructure_initiation.py

# Dashboard
Download the pbix file to your PC and select Refresh to display live data in the dashboard. The dashboard will be current after some time.

# Usage
To use this repository, you can clone it to your local machine: git clone 
https://github.com/bellamartirosyan/Stack-Overflow-Analysis/tree/main

# Methods 
I've been using a variety of sites over the last few months to study and construct the Stack Overflow database. BigQuery, a Google cloud-based data warehouse, was used to extract data from the Stack Overflow dataset in the beginning. I exported the necessary data from BigQuery to a JSON format using scripts and queries. I then used Google Drive, a cloud storage service offered by Google, to store and distribute the JSON files of the obtained data.
In addition, I constructed the database using a number of software applications, including Python and SQL. Building the Stack Overflow database required the use of both SQL, a relational database management system, and Python, a computer language frequently used for data analysis and manipulation. Stack Overflow's public data dump, which gathers a sizable amount of user-generated content from the Stack Overflow website, as well as the Stack Exchange Data Explorer, which gives access to data from all Stack Exchange websites, including Stack Overflow, were additional datasets I used to create the database.
In order to study and create the Stack Overflow database, I made use of a variety of tools, databases, and datasets. I was able to extract, save, and analyze the data with the help of these resources, and I want to use them again in the future to improve the database. I was able to build a comprehensive database that is useful to a variety of consumers by making use of these resources.

# Keywords
Stack Overflow, DEA, BigQuery
# Contributing
You can fork this repository and submit a pull request with your changes if you wish to add to it. Please be sure to utilize your Google Drive and Cloud login information, include tests to your code, and adhere to the current code style.

# Credentials
The project also has Google Cloud/Drive Credentials that are secret files and are delivered by email. Please, download the files from the email message and place the files in the project folder when running the code. The secret files are:
- velvety-ring-349218-0b355d98e0e7.json: Google Cloud Service Account credentials
- client_secrets.json: Google Cloud Client ID credentials
- velvety-ring-349218-f9ffa94dc6a5.json: Google Drive credentials


