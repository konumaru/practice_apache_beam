import os
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions


GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
JOB_NAME = os.environ["JOB_NAME"]


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input",
            default=f"gs://{GCS_BUCKET_NAME}/practice_apache_beam/step02/input.txt",
            help="Input for the pipeline",
        )

        parser.add_argument(
            "--output",
            default=f"gs://{GCS_BUCKET_NAME}/practice_apache_beam/step02/output.txt",
            help="Output for the pipeline",
        )


class ComputeWordLength(beam.DoFn):
    """文字数を求める嫌韓処理"""

    def __init__(self):
        pass

    def process(self, element):
        yield len(element)


def run():
    options = MyOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = (
        f"gs://{GCS_BUCKET_NAME}/practice_apache_beam/binaries"
    )
    google_cloud_options.temp_location = (
        f"gs://{GCS_BUCKET_NAME}/practice_apache_beam/temp"
    )
    # ワーカーオプション, 自動スケーリングを有効化する
    options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"

    p = beam.Pipeline(options=options)

    (
        p
        | "ReadFromText" >> beam.io.ReadFromText(options.input)
        | "ComputeWordLength" >> beam.ParDo(ComputeWordLength())
        | "WriteToText" >> beam.io.WriteToText(options.output, file_name_suffix=".txt")
    )

    p.run()


if __name__ == "__main__":
    run()
