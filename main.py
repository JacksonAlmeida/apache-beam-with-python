from typing import Any

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.runners.runner import PipelineResult


def create_pipeline() -> PipelineResult:
    pipe: Pipeline = beam.Pipeline()

    pCollection: Any = (
            pipe
            | "importar dados" >> beam.io.ReadFromText("./voos_sample.csv", skip_header_lines=1)
            | "separar por virgulas" >> beam.Map(lambda record: record.split(","))
            | "Mostrar resultado" >> beam.Map(print)
                   )

    return pipe.run()

if __name__ == '__main__':
    print('Curso Apache Beam')
    create_pipeline().wait_until_finish()