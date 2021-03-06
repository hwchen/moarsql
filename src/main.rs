use failure::{Error, bail, format_err};
use itertools::{join, repeat_n};
use serde_derive::Deserialize;
use std::convert::TryInto;
use std::fmt::{self, Display};
use std::path::PathBuf;
use structopt::StructOpt;

const DEFAULT_INDENT: &str = "  ";

fn main() -> Result<(), Error> {
    let opt = CliOpt::from_args();

    let path = opt.filepath;
    let indent = opt.indent.unwrap_or_else(|| DEFAULT_INDENT.to_string());

    let input = std::fs::read_to_string(&path)?;

    let statement_config: StatementConfig = toml::from_str(&input)?;

    let statement: Statement = statement_config.try_into()?;

    statement.validate()?;

    let sql = statement.clickhouse_sql(&indent, opt.reverse_nesting);

    println!("{}", sql);

    Ok(())
}

#[derive(Debug, Deserialize)]
struct StatementConfig {
    create_table: Option<String>,
    joins: Option<Vec<String>>,
    selects: Vec<SelectConfig>,
    #[serde(default)]
    #[serde(rename="join_type")]
    global_join_type: JoinType,
}

#[derive(Debug, Deserialize)]
struct SelectConfig {
    table_name: String,
    projections: Vec<String>,
    group_by: Option<String>,
    where_clause: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Statement {
    create_table: Option<String>,
    joins: Vec<(String, JoinType)>,
    selects: Vec<Select>,
}

impl Statement {
    fn validate(&self) -> Result<(), Error> {
        // joins should have len one less than selects
        if self.joins.len() != self.selects.len() - 1 {
            bail!("joins len must be one less than selects");
        }

//        // check that joins are referencing a col (aliased) on
//        // both tables
//        for (i, selects) in self.selects.windows(2).enumerate() {
//            let join_tuple = &self.joins[i]; // should never panic, len checked above
//
//            // if joining on a tuple
//            let leading_char = join_tuple.chars().nth(0)
//                .ok_or_else(|| format_err!("empty join value not allowed"))?;
//
//            let tuple_cols = if leading_char == '(' {
//                join_tuple.trim_start_matches('(').trim_end_matches(')').split(',')
//                    .map(|s| s.trim().to_owned())
//                    .collect()
//            } else {
//                vec![join_tuple.to_owned()]
//            };
//
//            let can_join = tuple_cols.iter()
//                .all(|col| {
//                    selects[0].aliased_projections().contains(col)
//                    && selects[1].aliased_projections().contains(col)
//                });
//
//            if !can_join {
//                bail!("join must match a col or col alias for both tables: {}", join_tuple);
//            }
//        }

        Ok(())
    }

    fn clickhouse_sql(&self, indent: &str, reverse_nesting: bool) -> String {
        let indent_level = if self.create_table.is_some() {
            1
        } else {
            0
        };

        let mut selects_working = self.selects.clone();
        let mut joins_working = self.joins.clone();

        let res = Self::sql_subquery(
            &mut selects_working,
            &mut joins_working,
            indent,
            indent_level,
            reverse_nesting,
        );

        if let Some(ref create_table) = self.create_table {
            format!("CREATE TABLE {} AS\n(\n{}\n)", create_table, res)
        } else {
            res
        }
    }

    fn sql_subquery(
        selects: &mut Vec<Select>,
        joins: &mut Vec<(String, JoinType)>,
        indent: &str,
        indent_level: usize,
        reverse_nesting: bool,
        ) -> String
    {
        let base_indent: String = repeat_n(indent, indent_level).collect();
        let plus_1_indent: String = repeat_n(indent, indent_level + 1).collect();

        // early return for no joins
        let (join_col, join_type) = if joins.is_empty() {
            let res = Self::select_sql(&selects[0], indent, indent_level);
            let res = res.trim().trim_start_matches("(\n").trim_end_matches(")").trim_end();
            return res.to_owned();
        } else {
            joins.remove(0)
        };

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

        if reverse_nesting {
            // first half of join
            let join_l = selects.remove(0);

            res.push_str(&Self::select_sql(&join_l, indent, indent_level + 1));

            res.push_str(&format!("\n{}ALL {} JOIN\n", plus_1_indent, join_type));

            // subqueries

            if selects.len() >= 2 {
                res.push_str(&format!("{}(\n", plus_1_indent));
                res.push_str(&Self::sql_subquery(
                    selects,
                    joins,
                    indent,
                    indent_level + 2,
                    reverse_nesting,
                ));
                res.push_str(&format!("\n{})", plus_1_indent));
            } else if selects.len() == 1 {
                let join_r = selects.remove(0);
                res.push_str(&Self::select_sql(&join_r, indent, indent_level + 1));
            }

            res.push_str(&format!("\n{}USING {}", plus_1_indent, join_col));
        } else {
            // second half of join pop
            let join_r = selects.remove(0);

            // subqueries

            if selects.len() >= 2 {
                res.push_str(&format!("{}(\n", plus_1_indent));
                res.push_str(&Self::sql_subquery(
                    selects,
                    joins,
                    indent,
                    indent_level + 2,
                    reverse_nesting,
                ));
                res.push_str(&format!("\n{})", plus_1_indent));
            } else if selects.len() == 1 {
                let join_l = selects.remove(0);
                res.push_str(&Self::select_sql(&join_l, indent, indent_level + 1));
            }

            res.push_str(&format!("\n{}ALL {} JOIN\n", plus_1_indent, join_type));

            // now write the right side of join
            res.push_str(&Self::select_sql(&join_r, indent, indent_level + 1));

            res.push_str(&format!("\n{}USING {}", plus_1_indent, join_col));
        }

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

        // joins are a little special. the join type can be specified using ::::inner, etc.
        let global_join_type = statement_config.global_join_type;

        let joins = statement_config.joins.unwrap_or(vec![]).iter()
            .map(|join_config| {
                let mut join_statement = join_config.split("::::");
                let join_col = join_statement.next()
                    .ok_or_else(|| format_err!("No join col"))?
                    .to_owned();
                let local_join_type = match join_statement.next() {
                    Some(s) => s.parse()?,
                    None => global_join_type.clone(), // this is already defaulted if not present, through cli
                };

                Ok((join_col, local_join_type))
            })
            .collect::<Result<Vec<(String, JoinType)>, Error>>()?;

        Ok(Self {
            create_table: statement_config.create_table,
            joins,
            selects,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Select{
    table_name: String,
    projections: Vec<ProjectionCol>,
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
                    col: col.trim().to_owned(),
                    alias: Some(alias.trim().to_owned()),
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

#[derive(Debug, Clone, Deserialize)]
enum JoinType {
    #[serde(alias="right")]
    #[serde(alias="RIGHT")]
    Right,

    #[serde(alias="left")]
    #[serde(alias="LEFT")]
    Left,

    #[serde(alias="inner")]
    #[serde(alias="INNER")]
    Inner,

    #[serde(alias="outer")]
    #[serde(alias="OUTER")]
    Outer,
}

impl Default for JoinType {
    fn default() -> Self {
        JoinType::Inner
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Outer => write!(f, "OUTER"),
        }
    }
}

impl std::str::FromStr for JoinType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "right" | "RIGHT" => Ok(JoinType::Right),
            "left"  | "LEFT"  => Ok(JoinType::Left),
            "inner" | "INNER" => Ok(JoinType::Inner),
            "outer" | "OUTER" => Ok(JoinType::Outer),
            _ => Err(format_err!("Could not parse string to JoinType"))
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name="moarsql")]
struct CliOpt {
    #[structopt(long="indent")]
    indent: Option<String>,

    #[structopt(parse(from_os_str))]
    filepath: PathBuf,

    #[structopt(long="reverse-nesting")]
    reverse_nesting: bool,
}

