import numpy as np


# Create a matrix from wrapped numpy arrays (workaround, will be fixed later)
def copy_to_matrix(wrapped_rows):

    #stacked_matrix = np.vstack([arr.flatten() for arr in wrapped])
    #return stacked_matrix

    return [row.flatten() for row in wrapped_rows]
