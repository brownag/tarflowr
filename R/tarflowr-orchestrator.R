#' Define a targets pipeline as a tarflowr work unit.
#'
#' This is a work unit constructor designed for hierarchical workflows. It
#' creates a configuration object that describes a sub-project to be run as a
#' single step within a larger `tarflowr` meta-pipeline.
#'
#' @details
#' The object created by this function is intended to be passed as an element in
#' the `work_units` list to `tarflowr_run()`, with `tarflowr_run_subproject()`
#' set as the `process_func`.
#'
#' @param project_dir The path to the directory containing the sub-project's
#'   `_targets.R` file.
#' @param result_target The unquoted name of the target within the sub-project
#'   that should be returned as the result of this work unit.
#' @param name (Optional) A short, descriptive name for this work unit, which
#'   will be used for naming branches in the main `tarflowr` pipeline. If
#'   `NULL`, the basename of the `project_dir` will be used.
#'
#' @return A structured list object (`tarflowr_work_unit`) containing the
#'   configuration for the sub-project.
#' @author Andrew G. Brown
#' @export
tarflowr_project <- function(project_dir, result_target, name = NULL) {
    if (is.null(name)) {
        name <- basename(project_dir)
    }

    # Capture the result target name as a character string
    result_target_chr <- rlang::as_name(rlang::enquo(result_target))

    structure(
        list(
            name = name,
            project_dir = normalizePath(project_dir, mustWork = TRUE),
            result_target = result_target_chr
        ),
        class = "tarflowr_sub_project"
    )
}

#' Run a `targets` sub-project
#'
#' This function is designed to be used as the `process_func` in a
#' `tarflowr_run()` call for hierarchical workflows. It takes a configuration
#' object from `tarflowr_project()` and executes the specified `targets`
#' pipeline.
#'
#' @details This function operates as a "black box" orchestrator for a
#' sub-project. It runs `targets::tar_make()` in a clean, separate R process
#' using `callr` to ensure the parent and child pipelines do not interfere with
#' one another. After the sub-project completes successfully, it reads the
#' specified result target and returns its value.
#'
#' @param config A work unit object created by `tarflowr_project()`.
#'
#' @return The value of the `result_target` from the sub-project.
#'
#' @author Andrew G. Brown
#' @export
#'
#' @examples
#' \dontrun{
#' # Example of a Hierarchical Workflow
#'
#' # Assume you have two complex `targets` projects located at:
#' #   - "./study_site_A" (which produces a target called `final_model_fit`)
#' #   - "./study_site_B" (which also produces a target called `final_model_fit`)
#'
#' # 1. Define the work units using the new helper function
#' work_to_do <- list(
#'   tarflowr_project(
#'     project_dir = "./study_site_A",
#'     result_target = final_model_fit,
#'     name = "site_A_run"
#'   ),
#'   tarflowr_project(
#'     project_dir = "./study_site_B",
#'     result_target = final_model_fit,
#'     name = "site_B_run"
#'   )
#' )
#'
#' # 2. Define a function to combine the final results
#' combine_model_fits <- function(model_fit_list) {
#'   names(model_fit_list) <- sapply(model_fit_list, function(x) x$site_name)
#'   dplyr::bind_rows(model_fit_list, .id = "source_project")
#' }
#'
#' # 3. Run the meta-orchestrator
#' all_fits <- tarflowr_run(
#'   work_units = work_to_do,
#'   process_func = run_targets_project, # Use the built-in function
#'   combine_func = combine_model_fits,
#'   project_dir = "_meta_analysis_project",
#'   packages = c("dplyr"), # Packages needed by combine_func
#'   workers = 2
#' )
#'
#' print(all_fits)
#' }
tarflowr_run_subproject <- function(config) {
    if (!inherits(config, "tarflowr_sub_project")) {
        stop("Input must be a sub-project configuration created by `tarflowr_project()`.", call. = FALSE)
    }

    required_pkgs <- c("callr", "targets", "cli")
    for (pkg in required_pkgs) {
        if (!requireNamespace(pkg, quietly = TRUE)) {
            stop(paste("Package '", pkg, "' is required for this function."), call. = FALSE)
        }
    }

    cli::cli_alert_info("Starting sub-project: {.strong {config$name}}")

    callr::r(
        func = function(project_path) {
            targets::tar_make()
        },
        args = list(project_path = config$project_dir),
        wd = config$project_dir,
        show = TRUE,
        spinner = FALSE
    )

    cli::cli_alert_info("Sub-project {.strong {config$name}} complete. Reading result target...")

    result <- targets::tar_read_raw(
        name = config$result_target,
        store = file.path(config$project_dir, "_targets")
    )

    return(result)
}
