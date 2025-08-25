# Implementation of a Bibliometric Database for Greek Computer Science Departments

## Overview

This project presents the design and implementation of a complete backend system for the collection, storage, and analysis of bibliometric data from Google Scholar.
The system was developed as part of an undergraduate thesis at the International Hellenic University (Department of Information and Electronic Engineering).

The goal is to provide Greek Computer Science Departments with a scalable, automated, and reusable platform for evaluating research output through bibliometric indicators (h-index, i10-index, citations, publications, etc.).

## Features

**Automated Web Scraper (Python & Selenium):**
  - Extracts faculty data (publications, citations, h-index, i10-index, yearly citation graphs) from Google Scholar.
  - Handles encoding issues, validation, and error logging.

**Relational Database (MySQL):**
  - Normalized schema in 3NF with triggers and history tables.
  - Stores publications, staff data, statistics, and year-by-year citations.

**Stored Procedures:**
  - Predefined SQL functions to calculate per-staff and per-department metrics.
  - Supports aggregated statistics and comparative evaluations.

**RESTful API (PHP):**
  - Secure token-based authentication.
  - Endpoints for accessing staff, department, and publication metrics.
  - Supports GZIP compression for improved performance.

**Extensible Architecture:**
  - Modular design for easy integration with front-end applications or additional data sources.
  - Future-ready for UI dashboards or educational policy tools.

## Technologies Used

- **Python** (Selenium, Pandas, MySQL Connector, dotenv, logging)

- **MySQL** (Workbench, HeidiSQL)

- **PHP** (REST API development)

- **Linux Server** (SSH, SCP, cron jobs for automation)

- **Postman** (API testing)

- **Development Tools:** IntelliJ IDEA, VS Code, XeLaTeX, Overleaf
