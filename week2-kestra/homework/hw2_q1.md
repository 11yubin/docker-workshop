id: hw2_Q1
namespace: zoomcamp
description: |
  The CSV Data used in the course: https://github.com/DataTalksClub/nyc-tlc-data/releases

variables:
  taxi: "yellow"
  year: "2020"
  month: "12"

  file: "{{vars.taxi}}_tripdata_{{vars.year}}-{{vars.month}}.csv"
  staging_table: "public.{{vars.taxi}}_tripdata_staging"
  table: "public.{{vars.taxi}}_tripdata"

tasks:
  - id: extract
    type: io.kestra.plugin.scripts.shell.Commands
    outputFiles:
      - "*.csv"
    taskRunner:
      type: io.kestra.plugin.core.runner.Process
    commands:
      - wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{vars.taxi}}/{{render(vars.file)}}.gz | gunzip > {{render(vars.file)}}

  - id: analyze_file
    type: io.kestra.plugin.scripts.python.Script
    containerImage: python:3.9-slim
    beforeCommands:
      - pip install pandas
    warningOnStdErr: false
    inputFiles:
      data.csv: "{{ outputs.extract.outputFiles[vars.taxi ~ '_tripdata_' ~ vars.year ~ '-' ~ vars.month ~ '.csv'] }}"
    script: |
      import pandas as pd
      import os

      print(f"--- Processing: {{vars.taxi}} / {{vars.year}}-{{vars.month}} ---")
      
      file_size_bytes = os.path.getsize('data.csv')
      file_size_mib = file_size_bytes / (1024 * 1024)
      print(f"👉 [Q1] Uncompressed File Size: {file_size_mib:.1f} MiB")