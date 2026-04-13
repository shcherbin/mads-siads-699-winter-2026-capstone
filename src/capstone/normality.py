import math
import pandas as pd
import pingouin as pg
import seaborn as sns
from matplotlib import pyplot as plt


def plot_distributions(data: pd.DataFrame, columns: list[str]) -> None:
    ncols = math.ceil(len(columns) ** 0.5)
    nrows = math.ceil(len(columns) / ncols)

    fig, axes = plt.subplots(nrows, ncols, figsize=(10, 8))
    axes = axes.flatten()

    for i, col in enumerate(columns):
        sns.histplot(data[col].dropna(), kde=True, ax=axes[i])
        axes[i].set_title(col)

    # remove empty plots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()


def run_multivariate_normality_test(data: pd.DataFrame, alpha: float = 0.05) -> None:
    return pg.multivariate_normality(data, alpha=alpha)

