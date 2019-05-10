use failure::{Error, bail, format_err};
use itertools::{join, repeat_n};
use serde_derive::Deserialize;
use std::convert::TryInto;

fn main() -> Result<(), Error> {
    let path = std::env::args().nth(1)
        .ok_or_else(|| format_err!("expected file path arg"))?;

    let input = std::fs::read_to_string(&path)?;

    let statement_config: StatementConfig = toml::from_str(&input)?;

    let statement: Statement = statement_config.try_into()?;

    statement.validate()?;

    let sql = statement.clickhouse_sql();

    println!("{}", sql);

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
    where_clause: Option<String>,
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
        let indent_level = 0;
        let indent = "  ";

        let mut selects_working = self.selects.clone();
        let mut joins_working = self.joins.clone();

        let res = Self::sql_subquery(
            &mut selects_working,
            &mut joins_working,
            indent,
            indent_level,
        );

        res
    }

    fn sql_subquery(
        selects: &mut Vec<Select>,
        joins: &mut Vec<String>,
        indent: &str,
        indent_level: usize
        ) -> String
    {
        let base_indent: String = repeat_n(indent, indent_level).collect();
        let plus_1_indent: String = repeat_n(indent, indent_level + 1).collect();

        let join_col = joins.remove(0);

        let mut res = format!("{}SELECT\n", base_indent);

        // for each subquery's all cols, remove the duplicates (keep first)
        let mut col_set = std::collections::HashSet::new();
        let all_cols = selects.iter()
            .flat_map(|select| {
                select.aliased_projections().into_iter()
            })
            .filter(|alias| {
                col_set.insert(alias.clone())
            });

        let separator = format!(",\n{}", plus_1_indent);
        let all_cols_str = format!("{}{}",
            plus_1_indent,
            join(all_cols, &separator)
        );
        res.push_str(&all_cols_str);
        res.push_str(&format!("\n{}FROM\n", base_indent));

        // first half of join
        let join_l = selects.remove(0);

        res.push_str(&Self::select_sql(&join_l, indent, indent_level + 1));

        res.push_str(&format!("\n{}ALL INNER JOIN\n", plus_1_indent));

        // subqueries

        if selects.len() >= 2 {
            res.push_str(&format!("{}(\n", plus_1_indent));
            res.push_str(&Self::sql_subquery(
                selects,
                joins,
                indent,
                indent_level + 2,
            ));
            res.push_str(&format!("\n{})", plus_1_indent));
        } else if selects.len() == 1 {
            let join_r = selects.remove(0);
            res.push_str(&Self::select_sql(&join_r, indent, indent_level + 1));
        }

        res.push_str(&format!("\n{}USING {}", plus_1_indent, join_col));

        res
    }

    fn select_sql(select: &Select, indent: &str, indent_level: usize) -> String {
        let base_indent: String = repeat_n(indent, indent_level).collect();
        let plus_1_indent: String = repeat_n(indent, indent_level + 1).collect();
        let plus_2_indent: String = repeat_n(indent, indent_level + 2).collect();

        let mut res = String::new();

        if select.projections.is_empty() {
            res.push_str(&format!("{}{}\n",
                plus_1_indent,
                select.table_name,
            ));
        } else {
            res.push_str(&format!("{}(\n{}SELECT\n{}",
                base_indent,
                plus_1_indent,
                plus_2_indent,
            ));

            let separator = format!(",\n{}", plus_2_indent);
            let select_cols = join(select.projections_sql(), &separator);
            res.push_str(&select_cols);
            res.push_str(&format!("\n{}FROM {}", plus_1_indent, select.table_name));

            if let Some(ref group_by) = select.group_by {
                res.push_str(&format!("\n{}GROUP BY {}", plus_1_indent, group_by));
            }
            if let Some(ref where_clause) = select.where_clause {
                res.push_str(&format!("\n{}WHERE {}", plus_1_indent, where_clause));
            }
            res.push_str(&format!("\n{})", base_indent));
        }

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

#[derive(Debug, Clone, Deserialize)]
struct Select{
    table_name: String,
    projections: Vec<ProjectionCol>,
    condition: Option<String>,
    group_by: Option<String>,
    where_clause: Option<String>,

}

impl Select {
    fn aliased_projections(&self) -> Vec<String> {
        self.projections.iter()
            .map(|p| p.aliased())
            .collect()
    }

    fn projections_sql(&self) -> Vec<String> {
        self.projections.iter()
            .map(|p| p.sql_string())
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
            where_clause: select_config.where_clause,

        })
    }
}

#[derive(Debug, Clone, Deserialize)]
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
