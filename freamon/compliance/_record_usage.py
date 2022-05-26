from freamon.compliance import ComplianceData

from freamon.templates import Output


class RecordUsage(ComplianceData):

    def _compute(self, pipeline):
        usages_per_source = {source_index: set() for source_index in range(len(pipeline.train_sources))}
        for polynomial in pipeline.output_lineage[Output.X_TRAIN]:
            for entry in polynomial:
                usages_per_source[entry.operator_id].add(entry.row_id)
        return usages_per_source
