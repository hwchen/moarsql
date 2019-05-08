use failure::{Error, format_err};
use serde_derive::Deserialize;
use std::convert::TryInto;

fn main() -> Result<(), Error> {
    let path = std::env::args().nth(1)
        .ok_or_else(|| format_err!("expected file path arg"))?;

    let input = std::fs::read_to_string(&path)?;

    let selects_config: SelectsConfig = toml::from_str(&input)?;

    println!("{:?}", selects_config);

    let selects: Selects = selects_config.try_into()?;

    println!("{:?}", selects);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct SelectsConfig {
    selects: Vec<SelectConfig>,
}

#[derive(Debug, Deserialize)]
struct SelectConfig {
    table_name: String,
    primary_key: String,
    projections: Vec<String>,
    condition: Option<String>,
    group_by: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Selects {
    selects: Vec<Select>,
}

impl std::convert::TryFrom<SelectsConfig> for Selects {
    type Error = Error;

    fn try_from(selects_config: SelectsConfig) -> Result<Self, Self::Error> {
        let selects: Result<_,_> = selects_config.selects
            .into_iter()
            .map(|sc| sc.try_into())
            .collect();
        let selects = selects?;

        Ok(Selects {
            selects,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Select{
    table_name: String,
    primary_key: String,
    projections: Vec<ProjectionCol>,
    condition: Option<String>,
    group_by: Option<String>,

}

impl std::convert::TryFrom<SelectConfig> for Select {
    type Error = Error;

    fn try_from(select_config: SelectConfig) -> Result<Self, Self::Error> {
        let projections: Result<_,_> = select_config.projections
            .iter()
            .map(|sc| sc.parse())
            .collect();
        let projections = projections?;

        Ok(Select {
            table_name: select_config.table_name,
            primary_key: select_config.primary_key,
            projections,
            condition: select_config.condition,
            group_by: select_config.group_by,

        })
    }
}

#[derive(Debug, Deserialize)]
struct ProjectionCol {
    col: String,
    alias: Option<String>,
}

impl std::str::FromStr for ProjectionCol {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ProjectionCol {
            col: "".to_owned(),
            alias: None,
        })
    }

}
