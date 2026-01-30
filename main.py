from typing import Any

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.runners.runner import PipelineResult

def findWords(word):
    return word in ("quatro", "um", "dois", "três")

def create_pipeline() -> PipelineResult:
    pipe: Pipeline = beam.Pipeline()


    pCollection: Any = (
            pipe
            | "importar dados" >> beam.io.ReadFromText("./voos_sample.csv", skip_header_lines=1)
            | "separar por virgulas" >> beam.Map(lambda record: record.split(","))
            | "Filtrar os dados por Los Angeles" >> beam.Filter(lambda record: record[3] == "LAX")
            # | "Mostrar resultado" >> beam.Map(print)
            | "Gravar resultados" >> beam.io.WriteToText("./voos.txt")
    )

    pCollectionPoema: Any = (
        pipe
        | "importar dados poema" >> beam.io.ReadFromText("./poema.txt", skip_header_lines=0)
        | "separar por espaços" >> beam.FlatMap(lambda x: x.split(" "))
        | "Filter palavras" >> beam.Filter(findWords)
        | "Mostrar Resultados" >> beam.Map(print)
        # | "Gravar resultados do poema" >> beam.io.WriteToText("./poema_result.txt")
    )
    return pipe.run()

if __name__ == '__main__':
    print('Curso Apache Beam')
    create_pipeline().wait_until_finish()