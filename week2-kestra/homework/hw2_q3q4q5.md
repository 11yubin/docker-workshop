id: hw2_Q2
namespace: zoomcamp
description: |
  The CSV Data used in the course: https://github.com/DataTalksClub/nyc-tlc-data/releases

inputs:
  - id: taxi
    type: SELECT
    displayName: Select taxi type
    values: [yellow, green]
    defaults: green
  
  - id: months
    type: ARRAY
    itemType: STRING
    displayName: Target Months
    defaults: 
      - "01"
      - "all"
  
  - id: year
    type: SELECT
    displayName: select year
    values: ["2017", "2018", "2019", "2020", "2021"]
    defaults: "2021"


# variables:
  # months: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

tasks:
  # - id: for_each_month
  #   type: io.kestra.plugin.core.flow.ForEach
  #   # 리스트를 values에 연결
  #   values: "{{ vars.months }}"
  #   # 가능한 만큼 동시 실행
  #   concurrencyLimit: 0

  #   # 반복할 작업들
  #   tasks:
  #   - id: get_data
  #     type: io.kestra.plugin.core.http.Download
  #     uri: "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{inputs.taxi}}_tripdata_{{inputs.year}}-{{askrun.value}}.csv.gz"

  - id: get_n_analyze_file
    type: io.kestra.plugin.scripts.python.Script
    containerImage: python:3.9-slim
    beforeCommands:
      - pip install pandas
    warningOnStdErr: false
    script: |
      import pandas as pd
      import gzip
      import shutil
      import os

      month_var = {{inputs.months}}
      months = []

      row_count = 0

      if "all" in month_var:
        for i in range(1,13):
          months.append("{:02d}".format(i))
      
      else: months = month_var

      for month in months:
        print(f"--- Processing: {{inputs.taxi}} / {{inputs.year}}-{month} ---")

        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{inputs.taxi}}_tripdata_{{inputs.year}}-{month}.csv.gz"

        monthly_count = 0
        # chunksize를 써서 메모리 터지는 것 방지
        for chunk in pd.read_csv(url, compression='gzip', iterator=True, chunksize=100000, low_memory=False):
          row_count += len(chunk)
        
      print(f"👉 {{inputs.taxi}} {{inputs.year}} Total Rows: {row_count}")