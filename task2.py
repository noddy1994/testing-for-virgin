import apache_beam as beam
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

import pandas as pd

import datetime

input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_path = 'output/results2'

beam_options = PipelineOptions(
    project='beam-project',
    job_name='beam-job'
)


class CustomTransform(beam.PTransform):
    def expand(self, input_coll):

        func = (
            input_coll
                | "FilterTransactions" >> beam.ParDo(FilterTransactions())
                | "FilterDates" >> beam.ParDo(FilterDates())
                | "SumPerKey" >> beam.CombinePerKey(sum)
        )

        return func

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

#pipeline with composite function

# with beam.Pipeline(options=beam_options) as p:

#     datas = (
#         p
#         | "ReadFile" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
#         | "DoTheParsingThing" >> beam.Map(parse_lines)
#         | "Composite" >> CustomTransform() #Composite
#         | "Format" >> beam.Map(lambda x: ",".join(map(str,list(x))))
#         | "Write" >> beam.io.WriteToText(
#             output_path, file_name_suffix='.csv',
#             header="date,total_amount"
#         )
#     )


#test pipeline fir composite function I/O
with TestPipeline() as tp:
    #input
    TRANSACTIONS = [
        ["2011-01-09 02:54:25 UTC","wallet00000e719adfeaa64b5a","wallet00001866cb7e0f09a890","25"],
        ["2017-08-31 17:00:09 UTC","wallet00001e494c12b3083634","wallet00005f83196ec58e4ffe","13700000023.08"],
        ["2017-08-31 17:00:09 UTC","wallet00001e494c12b3083634","wallet00005f83196ec58e4ffe","19"],
        ["2009-08-31 17:00:09 UTC","wallet00001e494c12b3083634","wallet00005f83196ec58e4ffe","30"],
        ]

    #expected out
    OUTPUT_EXPECTED = [("2011-01-09",25.0),("2017-08-31",13700000023.08)]

    test_output = tp | beam.Create(TRANSACTIONS) | CustomTransform()

    assert_that(test_output, equal_to(OUTPUT_EXPECTED), label='CheckOutput')

    tp.run()




    




