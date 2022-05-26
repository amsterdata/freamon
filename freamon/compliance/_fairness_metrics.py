import numpy as np

from freamon.compliance import ComplianceData
from freamon.templates import SourceType, Output


class FairnessMetrics(ComplianceData):

    def __init__(self, sensitive_attribute, non_protected_class):
        self.sensitive_attribute = sensitive_attribute
        self.non_protected_class = non_protected_class

    def _compute(self, pipeline):
        fact_table_index, fact_table_source = [
            (index, test_source) for index, test_source in enumerate(pipeline.test_sources)
            if test_source.source_type == SourceType.ENTITIES][0]

        is_in_non_protected = list(fact_table_source.data[self.sensitive_attribute] == self.non_protected_class)

        y_pred = pipeline.outputs[Output.Y_PRED]
        lineage_y_pred = pipeline.output_lineage[Output.Y_PRED]

        # Compute the confusion matrix per group
        y_test = pipeline.outputs[Output.Y_TEST]

        non_protected_false_negatives = 0
        non_protected_true_positives = 0
        non_protected_true_negatives = 0
        non_protected_false_positives = 0

        protected_false_negatives = 0
        protected_true_positives = 0
        protected_true_negatives = 0
        protected_false_positives = 0

        for index, polynomial in enumerate(lineage_y_pred):
            for entry in polynomial:
                if entry.operator_id == fact_table_source.operator_id:
                    # Positive ground truth label
                    if y_test[index] == 1.0:
                        if is_in_non_protected[entry.row_id]:
                            # if is_in_non_protected_by_row_id[entry.row_id]:
                            if y_pred[index] == 1.0:
                                non_protected_true_positives += 1
                            else:
                                non_protected_false_negatives += 1
                        else:
                            if y_pred[index] == 1.0:
                                protected_true_positives += 1
                            else:
                                protected_false_negatives += 1
                    # Negative ground truth label
                    else:
                        if is_in_non_protected[entry.row_id]:
                            # if is_in_non_protected_by_row_id[entry.row_id]:
                            if y_pred[index] == 1.0:
                                non_protected_false_positives += 1
                            else:
                                non_protected_true_negatives += 1
                        else:
                            if y_pred[index] == 1.0:
                                protected_false_positives += 1
                            else:
                                protected_true_negatives += 1

        return non_protected_true_negatives, non_protected_false_positives, non_protected_false_negatives, \
               non_protected_true_positives, protected_true_negatives, protected_false_positives, \
               protected_false_negatives, protected_true_positives

