from tsn_adapters.tasks.argentina.types import AvgPriceDF, CategoryMapDF, UncategorizedDF


def get_uncategorized_products(
    data: AvgPriceDF,
    category_map: CategoryMapDF,
) -> UncategorizedDF:
    """
    Get the products without category
    """
    diff_df = data[~data["id_producto"].isin(category_map["id_producto"])]

    # get data without id_producto (=null)
    data[data["id_producto"].isnull()]

    return UncategorizedDF(diff_df)