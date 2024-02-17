from itertools import product as iter_product
import math


def get_partition_data(variables, limit):
    """Get the number of batches and batch sizes for each variable."""
    batch_size_sets = {}
    number_of_batches_lists = []
    for var, values in variables.items():
        size_to_batches = {}

        for size in range(1, min(len(values) + 1, limit + 1)):
            nbr_of_batches = math.ceil(len(values) / size)
            # This ensures we only keep the largest size for each unique number of batches
            size_to_batches[nbr_of_batches] = size
        batch_size_sets[var] = size_to_batches
        number_of_batches_lists.append(list(size_to_batches.keys()))

    return batch_size_sets, number_of_batches_lists


def find_optimal_combination(variables, limit):
    """Optimized function to find the optimal batch sizes for each variable."""
    total_rows = math.prod([len(values) for values in variables.values()])
    lower_request_bound = math.ceil(total_rows / limit)
    if lower_request_bound == 1:
        return {var: len(values) for var, values in variables.items()}

    batch_size_sets, number_of_batches_lists = get_partition_data(variables, limit)

    best_combination = None
    min_request_count = float("inf")

    for combo in iter_product(*number_of_batches_lists):
        request_count = math.prod(combo)
        if request_count >= lower_request_bound and request_count < min_request_count:
            batch_sizes_product = math.prod(
                [batch_size_sets[var][nbr] for var, nbr in zip(variables.keys(), combo)]
            )
            if batch_sizes_product <= limit:
                min_request_count = request_count
                best_combination = combo
                if min_request_count == lower_request_bound:
                    break
    return {
        var: batch_size_sets[var][nbr]
        for var, nbr in zip(variables.keys(), best_combination)
    }


def split_into_batches(values, batch_size):
    """Split values into batches of up to batch_size, with the last batch potentially smaller."""
    nbr_of_lists = math.ceil(len(values) / batch_size)
    batches = [
        values[i * batch_size : (i + 1) * batch_size] for i in range(nbr_of_lists)
    ]
    return batches


def generate_all_combinations(variables, optimal_batch_sizes):
    """Generate all combinations of batches, one batch from each variable's batched lists of values."""
    # Split each variable's values into batches according to the optimal batch size
    all_batches = {
        var: split_into_batches(values, optimal_batch_sizes[var])
        for var, values in variables.items()
    }
    # Generate the Cartesian product of all batches to form the configurations
    configurations = list(iter_product(*all_batches.values()))

    # Convert tuple configurations to dictionary format
    configurations_dicts = []
    for configuration in configurations:
        config_dict = {
            var: batch for var, batch in zip(variables.keys(), configuration)
        }
        configurations_dicts.append(config_dict)

    return configurations_dicts


def get_request_configs(variables, limit, return_optimal_batch_sizes=False):
    """Get the optimal batch sizes and all possible combinations of batches.
    Args:
        variables (dict): A dictionary of variable names to lists of values.
        limit (int): The maximum number of rows to request.
        return_optimal_batch_sizes (bool): Whether to return the optimal batch sizes.
    Returns:
        dict or tuple: If return_optimal_batch_sizes is True, return a tuple of the optimal batch sizes and the request configurations. Otherwise, return the request configurations.
    """
    optimal_batch_sizes = find_optimal_combination(variables, limit)
    request_configs = generate_all_combinations(variables, optimal_batch_sizes)
    if return_optimal_batch_sizes:
        return optimal_batch_sizes, request_configs
    return request_configs
