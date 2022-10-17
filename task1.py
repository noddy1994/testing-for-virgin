import apache_beam as beam
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.options.pipeline_options import PipelineOptions

import pandas as pd

import datetime

input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_path = 'output/results1'

beam_options = PipelineOptions(
    project='beam-project',
    job_name='beam-job'
)


def parse_lines(element):
    return element.split(",")

#Remove unwanted transactions
class FilterTransactions(beam.DoFn):
    def process(self,element):
        amt = float(element[3])

        if amt > 20:
            yield element[:]
        else:
            pass

#Remove unwanted dates
class FilterDates(beam.DoFn):
    def process(self,element):

        date_format = "%Y-%m-%d %H:%M:%S %Z"
        date_element = element[0]

        timestamp = datetime.datetime.strptime(date_element, date_format)
        
        if timestamp.year > 2010:
            date = str(timestamp.date())
            yield (date,float(element[3]))
        else:
            pass


with beam.Pipeline(options=beam_options) as p:

    datas = (
        p
        | "ReadFile" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
        | "DoTheParsingThing" >> beam.Map(parse_lines)
        | "FilterTransactions" >> beam.ParDo(FilterTransactions())
        | "FilterDates" >> beam.ParDo(FilterDates())
        | "SumPerKey" >> beam.CombinePerKey(sum)
        | "Format" >> beam.Map(lambda x: ",".join(map(str,list(x))))
        | "Write" >> beam.io.WriteToText(
            output_path, file_name_suffix='.csv',
            header="date,total_amount"
        )

    )








    




