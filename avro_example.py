# step 1 - define avro schema
# step 2 -import required modules
import json
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def save_data_avro():
    try:
        # defined schema variable
        schema = avro.schema.parse(open("schema/flood.json", "rb").read())
        writer = DataFileWriter(open("data/floodinfo.avro", "wb"), DatumWriter(), schema)

        data = json.load(open("data/sample.json", "r"))
        for item in data:
            a_item = {
                "construction": str(item.get("construction")),
                "county": str(item.get("county")),
                "eq_site_deductible": item.get("eq_site_deductible"),
                "eq_site_limit": item.get("eq_site_limit"),
                "fl_site_deductible": item.get("fl_site_deductible"),
                "fl_site_limit": item.get("fl_site_limit"),
                "fr_site_deductible": item.get("fr_site_deductible"),
                "fr_site_limit": item.get("fr_site_limit"),
                "hu_site_deductible": item.get("hu_site_deductible"),
                "hu_site_limit": item.get("hu_site_limit"),
                "line": str(item.get("line")),
                "point_granularity": item.get("point_granularity"),
                "point_latitude": item.get("point_latitude"),
                "point_longitude": item.get("point_longitude"),
                "policyID": item.get("policyID"),
                "statecode": str(item.get("statecode")),
                "tiv_2011": item.get("tiv_2011"),
                "tiv_2012": item.get("tiv_2012")
            }

            writer.append(a_item)
        writer.close()

    except:
        raise


def read_data_from_avro():
    try:
        # defined schema variable
        # schema = avro.schema.parse(open("schema/flood.json", "rb").read())

        reader = DataFileReader(open("data/floodinfo2.avro", "rb"), DatumReader())
        for item in reader:
            print item
        reader.close()

    except:
        raise


if __name__ == '__main__':
    # save_data_avro()
    read_data_from_avro()
