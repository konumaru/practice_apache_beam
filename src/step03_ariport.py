import csv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input",
            default="../data/step03/airports.csv.gz",
            help="Input path for the pipeline",
        )

        parser.add_argument(
            "--output",
            default="../data/step03/output.csv",
            help="Output path for the pipeline",
        )


def addtimezone(lat, lon):
    try:
        import timezonefinder

        tf = timezonefinder.TimezoneFinder()
        return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
        # return (lat, lon, 'America/Los_Angeles') # FIXME
    except ValueError:
        return (lat, lon, "TIMEZONE")  # header


class ComputeWordLength(beam.DoFn):
    """文字数を求める嫌韓処理"""

    def __init__(self):
        pass

    def process(self, element):
        yield len(element)


def run():
    options = MyOptions()
    options.view_as(StandardOptions).runner = "DirectRunner"
    pipeline = beam.Pipeline(options=options)

    airports = (
        pipeline
        | beam.io.ReadFromText(options.input)
        | beam.Map(lambda line: next(csv.reader([line])))
        | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        | beam.io.WriteToText(options.output, file_name_suffix=".csv")
    )

    pipeline.run()


if __name__ == "__main__":
    run()
