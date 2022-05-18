import pycarol
from dotenv import load_dotenv


def main():
    load_dotenv(".env")
    carol = pycarol.Carol()

    data_model = "mdcurrency"
    sql_query = r"""
        with 
            currency_mdcurrency as (
                SELECT 
                    (select distinct mapping.uuid from (select * from (select row_number() over (partition by mdmId ORDER BY mdmCounterForEntity DESC) ranking, * from stg_protheus_carol_mapping) where ranking = 1  and (mdmDeleted = false or mdmDeleted is null)) as mapping where mapping.company_group= stg.company_group and coalesce(mapping.company, '') = coalesce(stg.company, '') and coalesce(mapping.unity, '') = coalesce(stg.unity, '') and mapping.branch = stg.branch) as _orgid,
                    `labs-app-mdm-production.a_techfin`.hash(stg.protheus_pk, 'mdcurrency') as currency_id,
                    stg.name as name,
                    stg.currencyAlias as currencyAlias,
                    stg.protheus_pk as protheus_id,
                    CAST(stg.deleted as BOOL) as deleted,
                    stg.protheus_pk as erp_id,
                    `labs-app-mdm-production.a_techfin`.buildSourceEntityNames(stg.mdmConnectorId, 'currency') as mdmSourceEntityNames,
                    --metadata--
                from (select * except(ranking) from (select row_number() over (partition by protheus_pk ORDER BY mdmCounterForEntity DESC) ranking, * from stg_protheus_carol_currency
                --timestamp-- WHERE mdmCounterForEntity > {{start_from}}
                ) where ranking = 1 ) stg
            ),

            mdcurrency as (
                select *
                from currency_mdcurrency stg
            )

        select *
        from mdcurrency stg
    """

    cds = pycarol.CDSGolden(carol)
    task = cds.process_bigquery(sql_query,
                                dm_name=data_model,
                                save_cds_data=True,
                                delete_target_folder=False,
                                send_subscriptions=True,
                                send_realtime_records=False,
                                delete_realtime_records=False,
                                use_dataflow=False,
                                # Is "Save to BigQuery" the default option?
                                )

    print(task)


if __name__ == "__main__":
    main()
