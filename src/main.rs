use failure::{Error, bail, format_err};
use itertools::join;
use serde_derive::Deserialize;
use std::convert::TryInto;

fn main() -> Result<(), Error> {
    let path = std::env::args().nth(1)
        .ok_or_else(|| format_err!("expected file path arg"))?;

    let input = std::fs::read_to_string(&path)?;

    let statement_config: StatementConfig = toml::from_str(&input)?;

    let statement: Statement = statement_config.try_into()?;

    statement.validate()?;

    println!("{:#?}", statement);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct StatementConfig {
    joins: Vec<String>,
    selects: Vec<SelectConfig>,
}

#[derive(Debug, Deserialize)]
struct SelectConfig {
    table_name: String,
    projections: Vec<String>,
    condition: Option<String>,
    group_by: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Statement {
    joins: Vec<String>,
    selects: Vec<Select>,
}

impl Statement {
    fn validate(&self) -> Result<(), Error> {
        // joins should have len one less than selects
        if self.joins.len() != self.selects.len() - 1 {
            bail!("joins len must be one less than selects");
        }

        // check that joins are referencing a col (aliased) on
        // both tables
        for (i, selects) in self.selects.windows(2).enumerate() {
            let join_col = &self.joins[i]; // should never panic, len checked above

            let can_join =
                selects[0].aliased_projections().contains(join_col)
                && selects[1].aliased_projections().contains(join_col);

            if !can_join {
                bail!("join must match a col or col alias for both tables");
            }
        }

        Ok(())
    }

    fn clickhouse_sql(&self) -> String {
        let mut res = String::new();

        // first do the final select; it's all cols from all selects
//        res.push_str(
//            format!("{}"
//        for 

        res
    }
}

impl std::convert::TryFrom<StatementConfig> for Statement {
    type Error = Error;

    fn try_from(statement_config: StatementConfig) -> Result<Self, Self::Error> {
        let selects: Result<_,_> = statement_config.selects
            .into_iter()
            .map(|sc| sc.try_into())
            .collect();
        let selects = selects?;

        Ok(Self {
            joins: statement_config.joins,
            selects,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Select{
    table_name: String,
    projections: Vec<ProjectionCol>,
    condition: Option<String>,
    group_by: Option<String>,

}

impl Select {
    fn aliased_projections(&self) -> Vec<String> {
        self.projections.iter()
            .map(|p| p.aliased())
            .collect()
    }
}

impl std::convert::TryFrom<SelectConfig> for Select {
    type Error = Error;

    fn try_from(select_config: SelectConfig) -> Result<Self, Self::Error> {
        let projections: Result<_,_> = select_config.projections
            .iter()
            .map(|sc| sc.parse())
            .collect();
        let projections = projections?;

        Ok(Self {
            table_name: select_config.table_name,
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
        match &s.split(" as ").collect::<Vec<_>>()[..] {
            &[col, alias] => {
                Ok(ProjectionCol {
                    col: col.to_owned(),
                    alias: Some(alias.to_owned()),
                })
            },
            &[col] => {
                Ok(ProjectionCol {
                    col: col.to_owned(),
                    alias: None,
                })
            },
            _ => Err(format_err!("could not parse projection col: {}", s))
        }
    }

}

impl ProjectionCol {
    fn sql_string(&self) -> String {
        if let Some(ref alias) = self.alias {
            format!("{} as {}", self.col, alias)
        } else {
            format!("{}", self.col)
        }
    }

    fn aliased(&self) -> String {
        if let Some(ref alias) = self.alias {
            alias.to_owned()
        } else {
            self.col.to_owned()
        }
    }
}
