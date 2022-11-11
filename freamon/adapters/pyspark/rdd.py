from pyspark.sql import Row


class TracedRDD:

    def __init__(self, rdd):
        self.rdd = rdd


    def take(self, num_items):
        return self.rdd.take(num_items)


    def count(self):
        return self.rdd.count()


    def filter(self, filter_func):
        filtered_rdd = self.rdd \
            .filter(lambda row_and_id: filter_func(row_and_id[0]))
        return TracedRDD(filtered_rdd)


    def select(self, columns):
        def __project_row(row, columns):
            row_dict = {column: row[column] for column in columns}
            return Row(**row_dict)

        projected_rdd = self.rdd \
            .map(lambda row_and_id: (__project_row(row_and_id[0], columns), row_and_id[1]))
        return TracedRDD(projected_rdd)


    def withColumn(self, column, map_func):
        def __extended_project_row(row, column, map_func):
            row_dict = row.asDict()
            row_dict[column] = map_func(row)
            return Row(**row_dict)

        projected_rdd = self.rdd \
            .map(lambda row_and_id: (__extended_project_row(row_and_id[0], column, map_func), row_and_id[1]))
        return TracedRDD(projected_rdd)


    def join(self, other, left_on, right_on):
        def paired_row(joined_rows):
            key, rows_with_polynomials = joined_rows
            left_row_with_polynomial, right_row_with_polynomial = rows_with_polynomials
            left_row, left_polynomial = left_row_with_polynomial
            right_row, right_polynomial = right_row_with_polynomial

            polynomial = left_polynomial.union(right_polynomial)
            row_dict = left_row.asDict()
            row_dict.update(right_row.asDict())

            return Row(**row_dict), polynomial

        keyed_rdd_left = self.rdd \
            .map(lambda row_and_id: (row_and_id[0][left_on], row_and_id))
        keyed_rdd_right = other.rdd \
            .map(lambda row_and_id: (row_and_id[0][right_on], row_and_id))

        joined_rdd = keyed_rdd_left \
            .join(keyed_rdd_right) \
            .map(lambda joined_rows: paired_row(joined_rows))

        return TracedRDD(joined_rdd)


    def randomSplit(self, train_ratio, seed):
        rdd1, rdd2 = self.rdd.randomSplit([train_ratio, 1.0 - train_ratio], seed)
        return TracedRDD(rdd1), TracedRDD(rdd2)


    def cache(self):
        self.rdd.cache()

