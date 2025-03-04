from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim, when, split, lower, array_contains, array_append


def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Assignment") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    data_folder = '/opt/spark/data'
    # read data
    facebook_df = (spark.read
                   .csv(data_folder + '/facebook_dataset.csv', header=True, inferSchema=True,
                        sep=',',
                        quote='\"',
                        escape='\\'
                        ))

    # facebook_df.show(5)

    # facebook_df.printSchema()
    google_df = (spark.read
                 .csv(data_folder + '/google_dataset.csv', header=True, inferSchema=True,
                      sep=',',
                      quote='\"',
                      escape='\\'
                      ))

    # google_df.printSchema()
    # google_df.show(5)

    website_df = (spark.read
                  .csv(data_folder + '/website_dataset.csv', header=True, inferSchema=True,
                       sep=';',
                       quote='\"',
                       escape='\\'))
    # website_df.printSchema()
    # website_df.show(5)

    # trim data and transform some columns to have lower characters
    google_df_trimmed = google_df.select(lower(trim(google_df['address'])).alias('address'),
                                         lower(trim(google_df['category'])).alias('category'),
                                         lower(trim(google_df['city'])).alias('city'),
                                         lower(trim(google_df['country_code'])).alias('country_code'),
                                         lower(trim(google_df['country_name'])).alias('country_name'),
                                         trim(google_df['name']).alias('name'),
                                         trim(google_df['phone']).alias('phone'),
                                         trim(google_df['phone_country_code']).alias('phone_country_code'),
                                         lower(trim(google_df['raw_address'])).alias('raw_address'),
                                         trim(google_df['raw_phone']).alias('raw_phone'),
                                         lower(trim(google_df['region_code'])).alias('region_code'),
                                         lower(trim(google_df['region_name'])).alias('region_name'),
                                         trim(google_df['text']).alias('text'),
                                         lower(trim(google_df['zip_code'])).alias('zip_code'),
                                         lower(trim(google_df['domain'])).alias('domain'))

    facebook_df_trimmed = facebook_df.select(lower(trim(facebook_df['domain'])).alias('domain'),
                                             lower(trim(facebook_df['address'])).alias('address'),
                                             lower(trim(facebook_df['categories'])).alias('categories'),
                                             lower(trim(facebook_df['city'])).alias('city'),
                                             lower(trim(facebook_df['country_code'])).alias('country_code'),
                                             lower(trim(facebook_df['country_name'])).alias('country_name'),
                                             trim(facebook_df['description']).alias('description'),
                                             lower(trim(facebook_df['email'])).alias('email'),
                                             lower(trim(facebook_df['link'])).alias('link'),
                                             trim(facebook_df['name']).alias('name'),
                                             lower(trim(facebook_df['page_type'])).alias('page_type'),
                                             trim(facebook_df['phone']).alias('phone'),
                                             trim(facebook_df['phone_country_code']).alias('phone_country_code'),
                                             lower(trim(facebook_df['region_code'])).alias('region_code'),
                                             lower(trim(facebook_df['region_name'])).alias('region_name'),
                                             lower(trim(facebook_df['zip_code'])).alias('zip_code'))

    website_df_trimmed = website_df.select(lower(trim(website_df['root_domain'])).alias('root_domain'),
                                           lower(trim(website_df['domain_suffix'])).alias('domain_suffix'),
                                           lower(trim(website_df['language'])).alias('language'),
                                           trim(website_df['legal_name']).alias('legal_name'),
                                           lower(trim(website_df['main_city'])).alias('main_city'),
                                           lower(trim(website_df['main_country'])).alias('main_country'),
                                           lower(trim(website_df['main_region'])).alias('main_region'),
                                           trim(website_df['phone']).alias('phone'),
                                           trim(website_df['site_name']).alias('site_name'),
                                           lower(trim(website_df['tld'])).alias('tld'),
                                           lower(trim(website_df['s_category'])).alias('s_category'))

    # verify if exist rows with name null
    # print(google_df_trimmed.filter(col('name').isNull()).count())
    # google_df_trimmed.filter(col('name').isNull()).show()

    # verify if exist rows with domain null
    # print(google_df_trimmed.filter(col('domain').isNull()).count())

    # count number of companies with the same domain
    domains = google_df_trimmed.groupBy(col('domain')).agg(count('domain').alias('number')).select(col('domain'),
                                                                                                   col('number'))

    # rows with the domain like others but with no name
    # print(google_df_trimmed.join(domains,'domain').filter((col('number') > 1) & (col('name').isNull())).count())
    # google_df_trimmed.join(domains, 'domain').filter((col('number') > 1) & (col('name').isNull())).show()

    google_df_trimmed_cleared = (google_df_trimmed.join(domains, 'domain')
                                 .filter(~((col('number') > 1) & (col('name').isNull()))).drop('number'))

    # verify companies with name and with country code but not with country name
    # print(google_df_trimmed_cleared.filter(col('country_code').isNotNull() & col('country_name').isNull()).count())

    # verify companies with name and with country name but not with country code
    # print(google_df_trimmed_cleared.filter(col('country_code').isNull() & col('country_name').isNotNull()).count())

    # verify if facebook pages exist without domain
    # print(facebook_df_trimmed.filter(col('domain').isNull()).count())

    domains_facebook = (facebook_df_trimmed.groupBy(col('domain'))
                        .agg(count('domain').alias('number')).select(col('domain'), col('number')))

    # I don't have rows without name and multiple domains in the dataset
    # print((facebook_df_trimmed.join(domains_facebook, 'domain')
    #  .filter(((col('number') > 1) & (col('name').isNull()))).drop('number')).count())

    # remove elements with the domain null
    website_df_trimmed_clear = website_df_trimmed.filter(col('root_domain').isNotNull())

    # see if the datasets have a valid domain with '.'
    # print(website_df_trimmed_clear.filter(~col('root_domain').contains('.')).count())
    # print(facebook_df_trimmed.filter(~col('domain').contains('.')).count())
    # print(google_df_trimmed_cleared.filter(~col('domain').contains('.')).count())

    # delete domains without '.'
    website_df_trimmed_clear = website_df_trimmed_clear.filter(col('root_domain').contains('.'))
    facebook_df_trimmed = facebook_df_trimmed.filter(col('domain').contains('.'))
    google_df_trimmed_cleared = google_df_trimmed_cleared.filter(col('domain').contains('.'))

    # language is not a valid column to join with google_df and facebook_df
    # (website_df_trimmed_clear.groupBy('language', 'main_country').agg(count('root_domain').alias('number'))
    #  .select('language', 'main_country','number').show())

    # tld is not a valid column to join
    # (website_df_trimmed_clear.groupBy('tld', 'main_country').agg(count('root_domain').alias('number'))
    #   .select('tld', 'main_country','number').show())

    # search for connection between facebook_df and google_df
    # print(facebook_df_trimmed.alias('a').join(google_df_trimmed_cleared.alias('b'),
    #                                          col('a.domain') == col('b.domain'), 'full')
    #      .filter(col('a.address').isNotNull() &
    #              ((col('a.address') == col('b.address')) | (col('a.address') == col('b.raw_address')))).count())
    # print(facebook_df_trimmed.alias('a').join(google_df_trimmed_cleared.alias('b'),
    #                                          col('a.domain') == col('b.domain'), 'full')
    #      .filter(col('a.address').isNotNull()
    #              & (col('a.address') == col('b.raw_address'))).count())
    #

    final_df = (google_df_trimmed_cleared.alias('a').join(facebook_df_trimmed.alias('b'),
                                               (col('a.domain') == col('b.domain'))
                                               & ((col('a.name') == col('b.name')) | col('a.name').isNull()
                                                  | col('b.name').isNull())
                                               & ((col('a.zip_code') == col('b.zip_code')) | col(
                                                   'a.zip_code').isNull() |
                                                  col('b.zip_code').isNull())
                                               & ((col('a.address') == col('b.address')) | col('a.address').isNull() |
                                                  col('b.address').isNull())
                                               & ((col('a.phone') == col('b.phone')) | col('a.phone').isNull()
                                                  | col('b.phone').isNull())
                                               & ((col('a.city') == col('b.city')) | col('a.city').isNull() |
                                                  col('b.city').isNull())
                                               & ((col('a.country_name') == col('b.country_name')) |
                                                  col('a.country_name').isNull() | col('b.country_name').isNull()
                                                  | (col('a.country_code') == col('b.country_code')) |
                                                  col('a.country_code').isNull() | col('b.country_code').isNull())
                                               & ((col('a.region_name') == col('b.region_name'))
                                                  | col('a.region_name').isNull() | col('b.region_name').isNull()
                                                  | (col('a.region_code') == col('b.region_code'))
                                                  | col('a.region_code').isNull() | col('b.region_code').isNull())
                                               , "full")
     .join(website_df_trimmed_clear.alias('c'),
           (col('c.root_domain') == col('a.domain'))
           & ((col('a.city') == col('c.main_city')) | col('a.city').isNull()
              | col('c.main_city').isNull())
           & ((col('a.phone') == col('c.phone')) | col('a.phone').isNull()
              | col('c.phone').isNull())
           & ((col('a.country_name') == col('c.main_country')) | col('a.country_name').isNull()
              | col('c.main_country').isNull())
           & ((col('a.country_name') == col('c.main_country')) | col('a.country_name').isNull()
              | col('c.main_country').isNull())
           & ((col('a.region_name') == col('c.main_region')) | col('a.region_name').isNull()
              | col('c.main_region').isNull())
           & (((col('a.region_name') == col('c.main_region')) | col('a.region_name').isNull()
               | col('c.main_region').isNull()))
           & (((col('a.name') == col('c.site_name')) | col('a.name').isNull()
               | col('c.site_name').isNull()) | ((col('a.name') == col('c.legal_name'))
                                                 | (col('a.name').isNull() | col('c.legal_name').isNull())))
           , 'full')
     .select(
        col('b.phone_country_code').alias('phone_country_code'),
        when(col('b.description').isNotNull(), col('b.description'))
        .otherwise(col('a.text')).alias('description'),
        when(col('a.zip_code').isNotNull(), col('a.zip_code'))
        .otherwise(col('b.zip_code')).alias('zip_code'),
        when(col('a.region_code').isNotNull(), col('a.region_code'))
        .otherwise(col('b.region_code')).alias('region_code'),
        when(col('a.region_name').isNotNull(), col('a.region_name'))
        .when(col('c.main_region').isNotNull(), col('c.main_region'))
        .otherwise(col('b.region_name')).alias('region_name'),
        when(col('a.city').isNotNull(), col('a.city'))
        .when(col('c.main_city').isNotNull(), col('c.main_city'))
        .otherwise(col('b.city')).alias('city'),
        when(col('a.country_code').isNotNull(), col('a.country_code'))
        .otherwise(col('b.country_code')).alias('country_code'),
        when(col('a.country_name').isNotNull(), col('a.country_name'))
        .when(col('c.main_country').isNotNull(), col('c.main_country'))
        .otherwise(col('b.country_name')).alias('country_name'),
        when(col('a.address').isNotNull(), col('a.address'))
        .when(col('b.address').isNotNull(), col('b.address'))
        .otherwise(col('a.raw_address')).alias('address'),
        when(col('a.domain').isNotNull(), col('a.domain'))
        .when(col('c.root_domain').isNotNull(), col('c.root_domain'))
        .otherwise(col('b.domain')).alias('domain'),
        when(col('a.phone').isNotNull(), col('a.phone'))
        .when(col('c.phone').isNotNull(), col('c.phone'))
        .when(col('b.phone').isNotNull(), col('b.phone'))
        .otherwise(col('a.raw_phone')).alias('phone'),
        col('a.category').alias('category'),
        col('c.s_category').alias('s_category'),
        split(col('b.categories'), '[\|]').alias('multiple_categories'),
        when(col('a.name').isNotNull(), col('a.name'))
        .when(col('c.site_name').isNotNull(), col('c.site_name'))
        .when(col('b.name').isNotNull(), col('b.name'))
        .otherwise(col('c.legal_name')).alias('name'))
     .filter(col('domain').isNotNull())
     .withColumn('categories_1', when(array_contains(col('multiple_categories'), col('category'))
                                      | col('category').isNull(),
                                      col('multiple_categories'))
                 .otherwise(array_append(col('multiple_categories'), col('category'))))
     .withColumn('categories', when(array_contains(col('categories_1'), col('s_category'))
                                    | col('s_category').isNull(),
                                    col('categories_1'))
                 .otherwise(array_append(col('categories_1'), col('s_category'))))
     .drop('categories_1', 'category', 's_category', 'multiple_categories'))

    final_df.show()

    (final_df
     .withColumn("categories_array",col('categories').cast("string"))
     .drop('categories')
     .write
     .option('header', True).mode('overwrite')
     .csv(data_folder + '/final_output'))


if __name__ == "__main__":
    main()
