class SqlQueries:
    stage_temperature_create = """
        CREATE TABLE IF NOT EXISTS stage_temperature(
            dt DATE,
            average_temperature FLOAT,
            average_temperature_uncertainty FLOAT,
            country TEXT
        );
    """

    stage_agri_chem_create = """
        CREATE TABLE IF NOT EXISTS stage_agri_chemical (
            series_id TEXT,
            name TEXT,
            units TEXT,
            f TEXT,
            copyright TEXT,
            source TEXT,
            geography TEXT,
            start INT,
            end INT,
            last_historical_period INT,
            last_updated TIMESTAMP,
            year INT,
            value FLOAT
        );
    """

    stage_crop_production_create = """
        CREATE TABLE IF NOT EXISTS stage_crop_production (
            area_code INT,
            area TEXT,
            item_code INT,
            item TEXT,
            element_code INT,
            element TEXT,
            year_code INT,
            year INT,
            unit TEXT,
            value FLOAT,
            flag_code TEXT,
            note TEXT
        );
    """

    stage_flag_create = """
        CREATE TABLE IF NOT EXISTS stage_flag (
          flag_code TEXT,
          flag_description TEXT
        );
    """

    temperature_create = """
        CREATE TABLE IF NOT EXISTS temperature (
          temperature_id INT IDENTITY (0,1) PRIMARY KEY,
          year INT SORTKEY,
          average_temperature FLOAT,
          average_temperature_uncertainty FLOAT
        );
    """

    agri_chem_prod_index_create = """
        CREATE TABLE IF NOT EXISTS agri_chem_prod_index (
          production_id INT IDENTITY (0,1) PRIMARY KEY,
          units TEXT,
          year INT SORTKEY,
          value FLOAT,
          last_historical_period INT,
          last_updated TIMESTAMP
        )
        DISTSTYLE all;
    """

    item_create = """
        CREATE TABLE IF NOT EXISTS item (
          item_code INT PRIMARY KEY SORTKEY,
          item_name TEXT
        )
        DISTSTYLE all;
    """

    element_create = """
        CREATE TABLE IF NOT EXISTS element (
          element_code INT PRIMARY KEY SORTKEY,
          element_name TEXT,
          unit TEXT
        )
        DISTSTYLE all;
    """

    flag_create = """
        CREATE TABLE IF NOT EXISTS flag (
          flag_code TEXT PRIMARY KEY SORTKEY,
          flag_description TEXT
        )
        DISTSTYLE all;
    """

    crop_production_create = """
        CREATE TABLE IF NOT EXISTS crop_production(
          crop_production_id INT IDENTITY (0,1) PRIMARY KEY,
          item_code INT NOT NULL REFERENCES item(item_code) DISTKEY,
          element_code INT NOT NULL REFERENCES element(element_code),
          year INT NOT NULL SORTKEY,
          value FLOAT,
          flag_code TEXT NOT NULL REFERENCES flag(flag_code),
          chem_production_id INT REFERENCES agri_chem_prod_index(production_id),
          temperature_id INT REFERENCES temperature(temperature_id)
        );
    """

    temperature_insert = """
        SELECT
            EXTRACT (year FROM dt),
            AVG(average_temperature),
            SUM(average_temperature_uncertainty)
        FROM stage_temperature
        WHERE country = 'United States'
        AND EXTRACT (year FROM dt) > 1960
        GROUP BY 1;
    """

    agri_chem_prod_index_insert = """
        SELECT DISTINCT
            units,
            year,
            value,
            last_historical_period,
            last_updated
            FROM stage_agri_chemical;
    """

    item_insert = """
        SELECT DISTINCT
            item_code,
            item
        FROM stage_crop_production;
    """

    element_insert = """
        SELECT DISTINCT
            element_code,
            element
        FROM stage_crop_production;
    """

    flag_insert = """
        SELECT DISTINCT
            flag_code,
            flag_description
        FROM stage_flag;
    """

    crop_production_insert = """
        SELECT DISTINCT
            scp.item_code,
            scp.element_code,
            scp.year,
            scp.value,
            scp.flag,
            scp.note,
            ac.production_id,
            t.temperature_id
        FROM stage_crop_production AS scp
        LEFT JOIN agri_chem_prod_index AS ac
            ON scp.year = ac.year
        LEFT JOIN temperature AS t
            ON scp.year = t.year
        WHERE scp.area = 'United States of America';
    """
