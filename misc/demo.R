library(tarflowr)
# 1. Define the work: a list of numbers
my_work <- as.list(1:10)

# 2. Define the processing function (works on one item)
square_a_number <- function(x) {
    # Add a delay to simulate real work
    Sys.sleep(1)
    return(x^2)
}

# 3. Define the combine function (works on the list of results)
sum_all_results <- function(results_list) {
    unlist(results_list) |> sum()
}

# 4. Run the workflow
final_sum <- tarflowr_run(
    work_units = my_work,
    process_func = square_a_number,
    combine_func = sum_all_results,
    project_dir = "_my_first_project",
    workers = 4
)

# The result is 385
print(final_sum)

# You can now inspect "_my_first_project" to see the generated
# _targets.R file and the _targets/ cache.

# To rerun and get the result instantly from the cache:
cached_sum <- tarflowr_run(
    work_units = my_work,
    process_func = square_a_number,
    combine_func = sum_all_results,
    project_dir = "_my_first_project",
    workers = 4,
    callr_function = callr::r_bg
)
print(cached_sum)

# tarflowr_load and tarflowr_read make it easy to load specific targets from a pipeline
tarflowr_load(my_first_project_result, "_my_first_project")
tarflowr_read(my_first_project_result, "_my_first_project")
