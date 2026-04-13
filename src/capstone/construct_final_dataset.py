import polars as pl
from functools import cached_property

from settings import load_settings

SETTINGS = load_settings()

# Constants for column names to avoid typos and ensure consistency across the codebase.

# ID columns - these are the columns that uniquely identify a package/repository and are used for joining datasets.
ID_REPOSITORY_NAME = "github_repo"
ID_PACKAGE_NAME = "package_name"

# Control (C) variable columns:

C_PACKAGE_DEPENDENCY_COUNT = "package_dependency_count"
C_PACKAGE_TOTAL_DOWNLOADS = "package_total_downloads"
C_REPOSITORY_AGE_IN_YEARS = "github_repo_age_in_years"
C_REPOSITORY_COMMIT_STALENESS_IN_DAYS = "github_repo_commit_staleness_in_days"
C_REPOSITORY_CONTRIBUTIONS_COUNT = "github_repo_contributions_count"
C_REPOSITORY_SIZE_IN_KB = "github_repo_size_in_kb"

ALL_CONTROL_VARIABLES = [
    C_PACKAGE_DEPENDENCY_COUNT,
    C_PACKAGE_TOTAL_DOWNLOADS,
    C_REPOSITORY_AGE_IN_YEARS,
    C_REPOSITORY_COMMIT_STALENESS_IN_DAYS,
    C_REPOSITORY_CONTRIBUTIONS_COUNT,
    C_REPOSITORY_SIZE_IN_KB,
]

# Predictor (P) variable columns:

P_BINARY_ARTIFACTS = "binary_artifacts" # Presence of binary artifacts in the repository.
P_BRANCH_PROTECTION = "branch_protection" # Implementation of branch protection rules.
P_CI_TESTS = "ci_tests" # Presence of continuous integration testing (e.g., GitHub Actions, Prow).
P_CII_BEST_PRACTICES = "cii_best_practices" # Adherence to the OpenSSF (formerly CII) Best Practices Badge at the passing, silver, or gold
P_CODE_REVIEW = "code_review" # Requirement of code reviews before merging changes.
P_CONTRIBUTORS = "contributors" # Involvement of contributors from at least two different organizations.
P_DANGEROUS_WORKFLOW = "dangerous_workflow"
P_DEPENDENCY_UPDATE_TOOL = "dependency_update_tool"
P_FUZZING = "fuzzing"
P_LICENSE = "license"
P_MAINTAINED = "maintained"
P_PACKAGING = "packaging" # Adherence to packaging best practices.
P_PINNED_DEPENDENCIES = "pinned_dependencies"
P_SAST = "sast" #  Use of static application security testing.
P_SECURITY_POLICY = "security_policy"
P_SIGNED_RELEASES = "signed_releases" # Use of signed releases.
P_TOKEN_PERMISSIONS = "token_permissions"

P_AGGREGATED_SCORE = "aggregated_score"

ALL_PREDICTOR_VARIABLES = [
    P_BINARY_ARTIFACTS,
    P_BRANCH_PROTECTION,
    P_CI_TESTS,
    P_CII_BEST_PRACTICES,
    P_CODE_REVIEW,
    P_CONTRIBUTORS,
    P_DANGEROUS_WORKFLOW,
    P_DEPENDENCY_UPDATE_TOOL,
    P_FUZZING,
    P_LICENSE,
    P_MAINTAINED,
    P_PACKAGING,
    P_PINNED_DEPENDENCIES,
    P_SAST,
    P_SECURITY_POLICY,
    P_SIGNED_RELEASES,
    P_TOKEN_PERMISSIONS,

    P_AGGREGATED_SCORE,
]

# Target (T) variable columns:
T_VULNERABILITY_COUNT = "vul_count"
T_MTTR = "mttr"
T_MTTU = "mttu"

ALL_TARGET_VARIABLES = [
    T_VULNERABILITY_COUNT,
    T_MTTR,
    T_MTTU,
]


class FinalDatasetConstructor:

    @cached_property
    def df_initial_dataset(self) -> pl.DataFrame:
        init_df = pl.scan_parquet(SETTINGS.initial_dataset_path)
        unique_repos = (
            init_df
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col(ID_REPOSITORY_NAME),
                )
                .unique()
                .collect()
        )
        return unique_repos

    @cached_property
    def df_feature_repo_age_and_commit_staleness(self) -> pl.DataFrame:
        data =  pl.read_parquet(f"{SETTINGS.feature_repo_age_and_staleness_path}")
        return (
            data
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col(ID_REPOSITORY_NAME),
                    pl.col("repo_age_years").alias(C_REPOSITORY_AGE_IN_YEARS),
                    pl.col("commit_staleness_days").alias(C_REPOSITORY_COMMIT_STALENESS_IN_DAYS),
                )
        )

    @cached_property
    def df_feature_dependency_count(self) -> pl.DataFrame:
        deps = pl.read_parquet(f"{SETTINGS.feature_dependency_count_without_version_path}")
        #return deps
        return (
            self.df_initial_dataset
                .join(deps, on=ID_PACKAGE_NAME, how="left")
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col("dependency_count").alias(C_PACKAGE_DEPENDENCY_COUNT),
                )
                # fill nulls with 0, since if a package is not present in the dependency count dataset, it means it has 0 dependencies
                .fill_null(0)
        )


    @cached_property
    def df_feature_total_downloads(self) -> pl.DataFrame:
        """Load the total downloads feature dataset and compute the sum total downloads per package.

        We compute the sum here since there are multiple entries for the same package (e.g., different versions),
        and we want a single value per package for the final dataset.
        """
        total_download_df = pl.scan_parquet(SETTINGS.feature_total_downloads_path)
        return (
            total_download_df
                .group_by(ID_PACKAGE_NAME)
                .agg(pl.col("total_downloads").sum().alias(C_PACKAGE_TOTAL_DOWNLOADS))
                .collect()
        )

    @cached_property
    def df_libraries_io(self) -> pl.DataFrame:
        librariesio = pl.scan_parquet(f"{SETTINGS.librariesio_path}/*.parquet")
        return (
            librariesio
                .select(
                    pl.col("full_name").alias(ID_REPOSITORY_NAME),
                    pl.col("contributions_count").alias(C_REPOSITORY_CONTRIBUTIONS_COUNT),
                    pl.col("size").alias(C_REPOSITORY_SIZE_IN_KB),
                )
                .collect()
        )

    @cached_property
    def df_feature_repo_contributions_and_size(self) -> pl.DataFrame:
        contributions_and_size = pl.scan_parquet(f"{SETTINGS.feature_repo_contributions_and_size_path}/*.parquet")
        return (
            contributions_and_size
                .select(
                    pl.col(ID_REPOSITORY_NAME),
                    pl.col("contributions_count").alias(C_REPOSITORY_CONTRIBUTIONS_COUNT),
                    pl.col("size_in_kb").alias(C_REPOSITORY_SIZE_IN_KB),
                )
                .collect()
        )

    @cached_property
    def df_ossf_scorecard(self) -> pl.DataFrame:
        data = pl.scan_parquet(SETTINGS.feature_ossf_scorecard_path)
        return (
            data
                .select(
                    pl.col("repo_name").alias(ID_REPOSITORY_NAME),

                    pl.col("Binary-Artifacts").alias(P_BINARY_ARTIFACTS),
                    pl.col("Branch-Protection").alias(P_BRANCH_PROTECTION),
                    pl.col("CI-Tests").alias(P_CI_TESTS),
                    pl.col("CII-Best-Practices").alias(P_CII_BEST_PRACTICES),
                    pl.col("Code-Review").alias(P_CODE_REVIEW),
                    pl.col("Contributors").alias(P_CONTRIBUTORS),
                    pl.col("Dangerous-Workflow").alias(P_DANGEROUS_WORKFLOW),
                    pl.col("Dependency-Update-Tool").alias(P_DEPENDENCY_UPDATE_TOOL),
                    pl.col("Fuzzing").alias(P_FUZZING),
                    pl.col("License").alias(P_LICENSE),
                    pl.col("Maintained").alias(P_MAINTAINED),
                    pl.col("Packaging").alias(P_PACKAGING),
                    pl.col("Pinned-Dependencies").alias(P_PINNED_DEPENDENCIES),
                    pl.col("SAST").alias(P_SAST),
                    pl.col("Security-Policy").alias(P_SECURITY_POLICY),
                    pl.col("Signed-Releases").alias(P_SIGNED_RELEASES),
                    pl.col("Token-Permissions").alias(P_TOKEN_PERMISSIONS),

                    pl.col("aggregate_score").alias(P_AGGREGATED_SCORE),
                    pl.col("vulnerabilities_detected").alias(T_VULNERABILITY_COUNT),
                )
                .collect()
        )

    @cached_property
    def df_mttu_mttr(self) -> pl.DataFrame:
        df = (
            pl.read_parquet(f"{SETTINGS.mttu_mttr_path}/*.parquet")
                .sort(["package_name", "snapshot_start"])
        )
        # latest snapshot (for avg_ttu)
        latest_avg_ttu = (
            df
            .group_by("package_name")
            .agg([
                pl.col("avg_ttu").last().alias("avg_ttu")
            ])
        )

        # latest non-zero avg_ttr
        latest_avg_ttr = (
            df
            .filter(pl.col("avg_ttr") != 0)
            .group_by("package_name")
            .agg([
                pl.col("avg_ttr").last().alias("avg_ttr")
            ])
        )

        return (
            latest_avg_ttu.join(latest_avg_ttr, on="package_name", how="left")
                .fill_null(0)
                .select(
                    pl.col("package_name").alias(ID_PACKAGE_NAME),
                    pl.col("avg_ttu").alias(T_MTTU),
                    pl.col("avg_ttr").alias(T_MTTR),
                )
        )

    @cached_property
    def df_final(self) -> pl.DataFrame:
        """Merge the initial dataset with the feature datasets to create the final dataset.
        """
        merged_df = (
            self.df_initial_dataset
            .join(self.df_feature_repo_age_and_commit_staleness, on=[ID_PACKAGE_NAME, ID_REPOSITORY_NAME], how="left")
            .join(self.df_feature_dependency_count, on=ID_PACKAGE_NAME, how="left")
            .join(self.df_feature_total_downloads, on=ID_PACKAGE_NAME, how="left")
            #.join(self.df_libraries_io, on=ID_REPOSITORY_NAME, how="left")
            .join(self.df_feature_repo_contributions_and_size, on=ID_REPOSITORY_NAME, how="left")
            .join(self.df_ossf_scorecard, on=ID_REPOSITORY_NAME, how="left")
            .join(self.df_mttu_mttr, on=ID_PACKAGE_NAME, how="left")
        )

        processed_df = (
            merged_df
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col(ID_REPOSITORY_NAME),

                    pl.col(C_PACKAGE_DEPENDENCY_COUNT),
                    pl.col(C_PACKAGE_TOTAL_DOWNLOADS),
                    pl.col(C_REPOSITORY_AGE_IN_YEARS),
                    pl.col(C_REPOSITORY_COMMIT_STALENESS_IN_DAYS),
                    pl.col(C_REPOSITORY_CONTRIBUTIONS_COUNT),
                    pl.col(C_REPOSITORY_SIZE_IN_KB),

                    pl.col(P_AGGREGATED_SCORE),
                    pl.col(P_BINARY_ARTIFACTS),
                    pl.col(P_BRANCH_PROTECTION),
                    pl.col(P_CI_TESTS),
                    pl.col(P_CII_BEST_PRACTICES),
                    pl.col(P_CODE_REVIEW),
                    pl.col(P_CONTRIBUTORS),
                    pl.col(P_DANGEROUS_WORKFLOW),
                    pl.col(P_DEPENDENCY_UPDATE_TOOL),
                    pl.col(P_FUZZING),
                    pl.col(P_LICENSE),
                    pl.col(P_MAINTAINED),
                    pl.col(P_PACKAGING),
                    pl.col(P_PINNED_DEPENDENCIES),
                    pl.col(P_SAST),
                    pl.col(P_SECURITY_POLICY),
                    pl.col(P_SIGNED_RELEASES),
                    pl.col(P_TOKEN_PERMISSIONS),

                    pl.col(T_VULNERABILITY_COUNT),
                    pl.col(T_MTTU),
                    pl.col(T_MTTR),
                )
                #.drop_nulls()
        )

        return processed_df

    @cached_property
    def df_rq1(self) -> pl.DataFrame:
        return (
            self.df_final
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col(ID_REPOSITORY_NAME),

                    pl.col(C_PACKAGE_DEPENDENCY_COUNT),
                    pl.col(C_PACKAGE_TOTAL_DOWNLOADS),
                    pl.col(C_REPOSITORY_AGE_IN_YEARS),
                    pl.col(C_REPOSITORY_COMMIT_STALENESS_IN_DAYS),
                    pl.col(C_REPOSITORY_CONTRIBUTIONS_COUNT),
                    pl.col(C_REPOSITORY_SIZE_IN_KB),

                    pl.col(P_AGGREGATED_SCORE),

                    pl.col(T_VULNERABILITY_COUNT),
                    pl.col(T_MTTR),
                    pl.col(T_MTTU),
                )
        )

    @cached_property
    def df_rq2(self) -> pl.DataFrame:
        return (
            self.df_final
                .select(
                    pl.col(ID_PACKAGE_NAME),
                    pl.col(ID_REPOSITORY_NAME),

                    pl.col(C_PACKAGE_DEPENDENCY_COUNT),
                    pl.col(C_PACKAGE_TOTAL_DOWNLOADS),
                    pl.col(C_REPOSITORY_AGE_IN_YEARS),
                    pl.col(C_REPOSITORY_COMMIT_STALENESS_IN_DAYS),
                    pl.col(C_REPOSITORY_CONTRIBUTIONS_COUNT),
                    pl.col(C_REPOSITORY_SIZE_IN_KB),

                    pl.col(P_BINARY_ARTIFACTS),
                    pl.col(P_BRANCH_PROTECTION),
                    pl.col(P_CI_TESTS),
                    pl.col(P_CODE_REVIEW),
                    pl.col(P_CONTRIBUTORS),
                    pl.col(P_DEPENDENCY_UPDATE_TOOL),
                    pl.col(P_LICENSE),
                    pl.col(P_MAINTAINED),
                    pl.col(P_PINNED_DEPENDENCIES),
                    pl.col(P_SAST),
                    pl.col(P_SECURITY_POLICY),

                    pl.col(T_VULNERABILITY_COUNT),
                    pl.col(T_MTTR),
                    pl.col(T_MTTU),
                )
        )



def main():
    constructor = FinalDatasetConstructor()
    final_df  = constructor.df_final
    print("Final dataset:")
    print(final_df.describe())

    final_df.write_parquet(SETTINGS.final_dataset_path)

    constructor.df_rq1.write_parquet(SETTINGS.research_question_1_dataset_path)
    constructor.df_rq2.write_parquet(SETTINGS.research_question_2_dataset_path)





if __name__ == "__main__":
    main()
