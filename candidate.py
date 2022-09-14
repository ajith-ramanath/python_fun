import pandas as pd
import json, fastavro
import urllib.request

CSV_PATH="https://raw.githubusercontent.com/implydata/candidate-exercises-public/master/Customer%20Success/SA/DataEngineeringProject/Applicant/DataSets/CityList.csv"
JSON_PATH="https://raw.githubusercontent.com/implydata/candidate-exercises-public/master/Customer%20Success/SA/DataEngineeringProject/Applicant/DataSets/CityListA.json"
AVRO_PATH="https://github.com/implydata/candidate-exercises-public/blob/master/Customer%20Success/SA/DataEngineeringProject/Applicant/DataSets/CityListB.avro?raw=true"

def read_csv():
    return pd.read_csv(CSV_PATH)


def read_json():
    citylist_json = None
    with urllib.request.urlopen(JSON_PATH) as url:
        citylist_json = json.load(url)
        #print(citylist_json)
    #return pd.read_json(json.dumps({'results': citylist_json}))
    return pd.DataFrame.from_records(citylist_json)
     

def read_avro():
    with urllib.request.urlopen(AVRO_PATH) as url:
        # Configure Avro reader
        reader = fastavro.reader(url)
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        return pd.DataFrame.from_records(records)


# Append all the entries into a single pandas DF, drop dups and sort by name
dedup_sorted_pdf = read_csv().append(read_json()).append(read_avro()).drop_duplicates(subset=['Name']).sort_values(by=['Name'])

# Write Name, CountryCode & Population to a csv file
header = ["Name", "CountryCode", "Population"]
dedup_sorted_pdf.to_csv('output.csv', columns = header, index=False)

print("Done & Dusted. Check 'output.csv' file...")