from freamon.compliance import ComplianceData


class ClassificationPipeline:

    def __init__(self, train_sources, train_source_lineage, test_sources, test_source_lineage,
                 outputs, output_lineage):

        self.train_sources = train_sources
        self.train_source_lineage = train_source_lineage
        self.test_sources = test_sources
        self.test_source_lineage = test_source_lineage
        self.outputs = outputs
        self.output_lineage = output_lineage


    def compute(self, compliance: ComplianceData):
        return compliance._compute(self)

