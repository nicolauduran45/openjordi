DATA_SOURCES = {
    "KAKEN": {
        "url": "https://kaken.nii.ac.jp/en/download/?kw=mental%20health&st=21&ot=csv&od=2&ct=all",
        "format": "csv",
        "parser": "csv"
    },
    "Swiss NSF": {#https://data.snf.ch/datasets
        "url": "https://stodppublicstorageprod.blob.core.windows.net/datasets/GrantWithAbstracts.csv",
        "format": "csv"
    },
    # "La Marató": {
    #     "url": "https://example.com/lamarato.xlsx",
    #     "format": "excel"
    # },
    "Arcadia": {
        "url": "https://arcadia-fund.files.svdcdn.com/production/WebData-08-2024.csv?dm=1724398894",
        "format": "csv",
        "parser": "csv"
    },
    "HRCS": {
        "url": "https://hrcsonline.net/wp-content/uploads/2024/01/UKHRA2022_HRCS_public_dataset_v1-2_30Jan2024.xlsx",
        "format": "excel",
        "parser": "excel"
    }
}
