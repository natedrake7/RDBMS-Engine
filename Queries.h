#pragma once
#include <string>

//select statement
const std::string selectActors = "SELECT * FROM dbo.Actors";

const std::string selectMasterDb = "SELECT * FROM dbo.sys_tables";

//select statement
const std::string selectMovies = "SELECT * FROM dbo.Movies";

//insert statement
const std::string insertActors = "INSERT INTO dbo.Actors(ActorName, ActorDesc, ActorAge) VALUES('Henry Cavill', 'kalispera', 43)";

const std::string insertMovies = "INSERT INTO dbo.Movies(ID, MovieName) VALUES(NEWID(), 'Batman: The Dark Knight')";

const std::string deleteMovies = "DELETE FROM movies.Movies WHERE MovieName = 'Batman: The Dark Knight'";

//create table
const std::string createMoviesTable = "CREATE TABLE dbo.Movies ( "
                                    "ID GUID NOT NULL, "
                                    "MovieName VARCHAR(800) NOT NULL"
                                 ")";

//create table
const std::string createActorsTable = "CREATE TABLE Actors ( "
                                 "ID INT PRIMARY KEY IDENTITY(1, 1) ,"
                                 "ActorName VARCHAR(255) NULL, "
                                 "ActorDesc VARCHAR(MAX),"
                                 " ActorAge INT NOT NULL"
                                 ")";

const std::string createDb = "CREATE DATABASE MoviesDb";

const std::string schemaCreate = "CREATE SCHEMA movies";

const std::string updateMovies = "UPDATE dbo.Movies SET MovieDesc = 'Batman Fights Bane' WHERE ID = 5";

const std::string updateActors = "UPDATE dbo.Actors SET ActorDesc = 'Henry Cavill is hot' WHERE ID <> 20";

const std::string actorsIndex = "CREATE INDEX idx_ActorsName ON dbo.Actors (ActorName)";