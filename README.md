# Yildiz-Engine module-database-hsqldb.

This is the official repository of the HSQLDB Database Module, part of the Yildiz-Engine project.
The database module is the derby implementation of the module database.

## Features

* HSQLDB support.

## Requirements

To build this module, you will need a the latest Java JDK, and Maven 3.

## Coding Style and other information

Project website:
https://engine.yildiz-games.be

Issue tracker:
https://yildiz.atlassian.net

Wiki:
https://yildiz.atlassian.net/wiki

Quality report:
https://sonarqube.com/overview?id=be.yildiz-games:module-database-hsqldb

## License

All source code files are licensed under the permissive MIT license
(http://opensource.org/licenses/MIT) unless marked differently in a particular folder/file.

## Build instructions

Go to your root directory, where you POM file is located.

Then invoke maven

	mvn clean install

This will compile the source code, then run the unit tests, and finally build a jar file.

## Usage

In your maven project, add the dependency

```xml
<dependency>
    <groupId>be.yildiz-games</groupId>
    <artifactId>module-database-hsqldb</artifactId>
    <version>LATEST</version>
</dependency>
```
Replace LATEST with the version number to use.

## Contact
Owner of this repository: Grégory Van den Borre