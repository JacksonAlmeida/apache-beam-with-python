import apache_beam as beam
from typing import Any
from apache_beam import Pipeline
from apache_beam.runners.runner import PipelineResult

def findWords(word):
    return word in ("quatro", "um", "dois", "três")

def create_pipeline() -> PipelineResult:
    pipe: Pipeline = beam.Pipeline()

    # model_cars: tuple[str, ...] = ("Gol", "Fiesta", "Civic", "Corolla", "Onix", "HB20", "Uno", "Sandero", "Prisma",
    #                                  "Argo")
    #
    # model_motorcycle: tuple[str, ...] = ("CG 160", "XRE 300", "Ninja 400", "CB 500", "Hornet 160R", "MT-07", "CB 1000R",
    #                                 "Ténéré 700", "R1", "Africa Twin")
    #
    # cars_pc = (
    #     pipe
    #     | "criando pcollection cars" >> beam.Create(model_cars)
    # )
    #
    # motorcycles_pc = (
    #     pipe
    #     | "criando pcollection motorcycles" >> beam.Create(model_motorcycle)
    # )
    #
    # veiches_pc = (
    # (cars_pc, motorcycles_pc) | beam.Flatten()
    # )
    #
    # veiches_pc | beam.Map(print)
    #
    # veiches_pc | beam.io.WriteToText("./veiches.txt")

    pCollection: Any = (
            pipe
            | "importar dados" >> beam.io.ReadFromText("./voos_sample.csv", skip_header_lines=1)
            | "separar por virgulas" >> beam.Map(lambda record: record.split(","))
            # | "Filtrar os dados por Los Angeles" >> beam.Filter(lambda record: record[9] == "LAX")
            | "pegar voo" >> beam.Filter(lambda record: int(record[8]) > 0)
            | "criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
            | "contar por keys" >> beam.combiners.Count.PerKey()
            | "Mostrar resultado" >> beam.Map(print)
            # | "Gravar resultados" >> beam.io.WriteToText("./voos.txt")
    )

    # pCollectionPoema: Any = (
    #     pipe
    #     | "importar dados poema" >> beam.io.ReadFromText("./poema.txt", skip_header_lines=0)
    #     | "separar por espaços" >> beam.FlatMap(lambda x: x.split(" "))
    #     | "Filter palavras" >> beam.Filter(findWords)
    #     | "Mostrar Resultados" >> beam.Map(print)
    #     # | "Gravar resultados do poema" >> beam.io.WriteToText("./poema_result.txt")
    # )
    return pipe.run()

if __name__ == '__main__':
    print('Curso Apache Beam')
    create_pipeline().wait_until_finish()