#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import time
import json

connection = MongoClient("localhost", 27017)
db = connection.enterprise       


def query1():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.libelle" : "Entretien corporel" , "fields.greffe" : "LYON"}).explain()["executionStats"]
		else:
			res = db.companies.find({"fields.libelle" : "Entretien corporel" , "fields.greffe" : "LYON"}).explain()["executionStats"]			
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")

def query2():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/",  "fields.greffe" :{"$in" :[ "LYON" , "NANTES"]} }).explain()["executionStats"]
		else:			
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/",  "fields.greffe" :{"$in" :[ "LYON" , "NANTES"]} }).explain()["executionStats"]			
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")

def query3():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/", "fields.siren": {"$gt": 803000000, "$lt": 803999999} }).explain()["executionStats"]
		else:			
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/", "fields.siren": {"$gt": 803000000, "$lt": 803999999} }).explain()["executionStats"]			
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")
			
def query4():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :  "/^2014/" }).explain()["executionStats"]
		else:
			
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^A/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :  "/^2014/" }).explain()["executionStats"]
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")

def query5():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :"/^2014/", "fields.code_ape": "/Z$/"  }).explain()["executionStats"]
		else:			
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :"/^2014/", "fields.code_ape": "/Z$/"  }).explain()["executionStats"]
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")

def query6():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/" , "fields.region" : "Lorraine" , "geometry.coordinates" : {"$elemMatch" : {"$gt": 49.0 , "$lt": 49.9}} , "fields.date" :  "/^2014/" , "fields.code_ape" : "/^0/"   }).explain()["executionStats"]
		else:
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/" , "fields.region" : "Lorraine" , "geometry.coordinates" : {"$elemMatch" : {"$gt": 49.0 , "$lt": 49.9}} , "fields.date" :  "/^2014/" , "fields.code_ape" : "/^0/"   }).explain()["executionStats"]
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")
			
def query7():
	for i in range(0,31):
		if (i==0):
			db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :"/^2014/", "fields.adresse": "/^A/", "geometry.coordinates" : {"$elemMatch" : {"$gt": 0 , "$lt": 4}} }).explain()["executionStats"]
		else:
			res =db.companies.find({"fields.forme_juridique" : "Société à responsabilité limitée" , "fields.denomination": "/^B/", "fields.siren": {"$gt": 803000000, "$lt": 803999999}, "fields.date" :"/^2014/", "fields.adresse": "/^A/", "geometry.coordinates" : {"$elemMatch" : {"$gt": 0 , "$lt": 4}} }).explain()["executionStats"]
			res1 = json.dumps(res['executionTimeMillis'])
			out_file.write(res1+"\n")
	
	
out_file = open("test.txt","w")
for x in range(1,8):
	if x==1:
		out_file.write("Query"+str(x)+"\n")
		query1()
	elif x==2:
		out_file.write("Query"+str(x)+"\n")
		query2()
	elif x==3:
		out_file.write("Query"+str(x)+"\n")
		query3()
	elif x==4:
		out_file.write("Query"+str(x)+"\n")
		query4()
	elif x==5:
		out_file.write("Query"+str(x)+"\n")
		query5()
	elif x==6:
		out_file.write("Query"+str(x)+"\n")
		query6()
	elif x==7:
		out_file.write("Query"+str(x)+"\n")
		query7()
	
	

out_file.close()


