import pandera as pa
from pandera.typing import DataFrame, Series


class SepaProductosDataModel(pa.DataFrameModel):
    """
    Pandera model for core SEPA productos data.
    """
    id_producto: Series[str]
    productos_descripcion: Series[str]
    productos_precio_lista: Series[float]
    date: Series[str]

    class Config(pa.DataFrameModel.Config):
        coerce = True
        strict = "filter"

    @staticmethod
    def from_full_data(
        data: DataFrame["FullSepaProductosDataModel"],
    ) -> DataFrame["SepaProductosDataModel"]:
        df = data[
            [
                "id_producto",
                "productos_descripcion",
                "productos_precio_lista",
                "date",
            ]
        ]
        return DataFrame[SepaProductosDataModel](df)


class FullSepaProductosDataModel(SepaProductosDataModel):
    """
    Pandera model for the full SEPA productos data, including additional details.
    """
    id_comercio: Series[str]
    id_bandera: Series[str]
    id_sucursal: Series[str]
    productos_ean: Series[str]
    productos_cantidad_presentacion: Series[float]
    productos_unidad_medida_presentacion: Series[str] = pa.Field(nullable=True)
    productos_marca: Series[str] = pa.Field(nullable=True)
    productos_precio_referencia: Series[float]
    productos_cantidad_referencia: Series[float]
    productos_unidad_medida_referencia: Series[str] = pa.Field(nullable=True)
    productos_precio_unitario_promo1: Series[float] = pa.Field(nullable=True)
    productos_leyenda_promo1: Series[str] = pa.Field(nullable=True)
    productos_precio_unitario_promo2: Series[float] = pa.Field(nullable=True)
    productos_leyenda_promo2: Series[str] = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        coerce = True
        add_missing_columns = False
        strict = True


class ProductDescriptionModel(pa.DataFrameModel):
    """
    Pandera model for unique product descriptions.
    """
    id_producto: Series[str]
    productos_descripcion: Series[str]

    @staticmethod
    def from_sepa_product_data(
        data: DataFrame[SepaProductosDataModel],
    ) -> DataFrame["ProductDescriptionModel"]:
        unique_data = data.drop_duplicates(subset="id_producto").reset_index(drop=True)
        return DataFrame[ProductDescriptionModel](unique_data[["id_producto", "productos_descripcion"]])

    class Config(pa.DataFrameModel.Config):
        add_missing_columns = False
        strict = "filter"


class SepaAvgPriceProductModel(ProductDescriptionModel):
    """
    Pandera model for product descriptions with average prices.
    """
    productos_precio_lista_avg: Series[float]
    date: Series[str]
    @staticmethod
    def from_sepa_product_data(
        data: DataFrame[SepaProductosDataModel],
    ) -> DataFrame["SepaAvgPriceProductModel"]:
        with_average_price = (
            data.groupby([ "id_producto", "date" ])
            .agg(
                {
                    "id_producto": "first",
                    "productos_precio_lista": "mean",
                    "productos_descripcion": "first",
                    "date": "first",
                }
            )
            .reset_index(drop=True)
        )

        with_average_price.rename(
            columns={
                "productos_precio_lista": "productos_precio_lista_avg",
            },
            inplace=True,
        )

        return DataFrame[SepaAvgPriceProductModel](with_average_price)
