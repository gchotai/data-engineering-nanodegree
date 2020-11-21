class SqlQueries:
        airport_code_table_insert = (""" SELECT code, name, country, state FROM airport_code_from_bucket """)

        cities_demographics_table_insert = (""" 
                                               SELECT city, 
                                                      male_population, 
                                                      female_population, 
                                                      total_population, 
                                                      state_code 
                                               FROM cities_demographics_from_bucket """)

        immigration_city_temp_table_insert = ("""
                                              SELECT immigration.i94yr,
                                                     immigration.i94mon,
                                                     immigration.i94cit,
                                                     immigration.i94mode,
                                                     immigration.i94visa,
                                                     city_temp.averagetemperature,
                                                     city_temp.city,
                                                     city_temp.country
                                                     FROM city_temp
                                                     JOIN immigration 
                                                     ON lower(city_temp.City) = lower(immigration.i94port) """)

        city_temp_table_insert = (""" SELECT avg(averagetemperature) as averagetemperature, city, country FROM city_temp_from_bucket group by city,country """)

        immigration_table_insert = (""" SELECT i94yr, i94mon, i94cit, i94port, i94mode, i94visa FROM immigration_from_bucket """)

  