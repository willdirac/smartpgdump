use std::{error::Error, fs, io::Write, path::Path, str::FromStr};

const SECTION_HEADER_BOUNDARY_PATTERN: &str = "\n--\n";

#[derive(Debug, Clone, PartialEq)]
pub enum ObjectType {
    Table,
    FkConstraint,
    Type,
    Trigger,
    Sequence,
    Function,
    Comment,
    DefaultAcl,
    Index,
    Extension,
    Schema,
    Domain,
    Default,
    Constraint,
    Acl,
    SequenceOwnedBy,
}

impl FromStr for ObjectType {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TABLE" => Ok(ObjectType::Table),
            "FK CONSTRAINT" => Ok(ObjectType::FkConstraint),
            "TYPE" => Ok(ObjectType::Type),
            "TRIGGER" => Ok(ObjectType::Trigger),
            "SEQUENCE" => Ok(ObjectType::Sequence),
            "FUNCTION" => Ok(ObjectType::Function),
            "COMMENT" => Ok(ObjectType::Comment),
            "DEFAULT ACL" => Ok(ObjectType::DefaultAcl),
            "INDEX" => Ok(ObjectType::Index),
            "EXTENSION" => Ok(ObjectType::Extension),
            "SCHEMA" => Ok(ObjectType::Schema),
            "DOMAIN" => Ok(ObjectType::Domain),
            "DEFAULT" => Ok(ObjectType::Default),
            "CONSTRAINT" => Ok(ObjectType::Constraint),
            "ACL" => Ok(ObjectType::Acl),
            "SEQUENCE OWNED BY" => Ok(ObjectType::SequenceOwnedBy),
            _ => Err(format!("Unknown object type: {}", s).into()),
        }
    }
}

impl ObjectType {
    pub fn as_str(&self) -> &str {
        match self {
            ObjectType::Table => "TABLE",
            ObjectType::FkConstraint => "FK CONSTRAINT",
            ObjectType::Type => "TYPE",
            ObjectType::Trigger => "TRIGGER",
            ObjectType::Sequence => "SEQUENCE",
            ObjectType::Function => "FUNCTION",
            ObjectType::Comment => "COMMENT",
            ObjectType::DefaultAcl => "DEFAULT ACL",
            ObjectType::Index => "INDEX",
            ObjectType::Extension => "EXTENSION",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Domain => "DOMAIN",
            ObjectType::Default => "DEFAULT",
            ObjectType::Constraint => "CONSTRAINT",
            ObjectType::Acl => "ACL",
            ObjectType::SequenceOwnedBy => "SEQUENCE OWNED BY",
        }
    }
}

#[derive(Debug)]
pub struct SchemaHeader {
    name: String,
    object_type: ObjectType,
    schema: String,
    owner: String,
}

#[derive(Debug)]
pub struct SchemaSection {
    header: SchemaHeader,
    body: String,
}

// instead of storing schema we could have a bin for each of the known body_types, these will need
// to be ordered for writing anyway and some we will want to combine
#[derive(Debug, Default)]
pub struct Schema {
    tables: Vec<SchemaSection>,
    types: Vec<SchemaSection>,
    functions: Vec<SchemaSection>,
    constraints: Vec<SchemaSection>,
    indexes: Vec<SchemaSection>,
    comments: Vec<SchemaSection>,
    general: Vec<SchemaSection>,
}

impl FromStr for SchemaHeader {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let content = s.strip_prefix("-- ").ok_or("Missing prefix")?;
        let parts: Vec<&str> = content.split("; ").collect();

        Ok(SchemaHeader {
            name: parts[0]
                .strip_prefix("Name: ")
                .ok_or("Missing Name")?
                .split("(")
                .next()
                .ok_or("Couldn't remove parentheses")?
                .to_string(),
            object_type: parts[1]
                .strip_prefix("Type: ")
                .ok_or("Missing Type")?
                .parse::<ObjectType>()?,
            schema: parts[2]
                .strip_prefix("Schema: ")
                .ok_or("Missing Schema")?
                .to_string(),
            owner: parts[3]
                .strip_prefix("Owner: ")
                .ok_or("Missing Owner")?
                .to_string(),
        })
    }
}

impl FromStr for Schema {
    type Err = Box<dyn Error>;
    // we want to split on --\n
    // example input:
    // --
    // -- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: linkedin; Owner: postgres
    // --
    //
    // ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA linkedin GRANT ALL ON FUNCTIONS TO admin;
    // ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA linkedin GRANT ALL ON FUNCTIONS TO linkedin;
    //
    //
    // --
    // -- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: -; Owner: postgres
    // --
    //
    // ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE ALL ON FUNCTIONS FROM PUBLIC;
    //
    //
    // --
    // -- PostgreSQL database dump complete
    // --
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut schema = Schema::default();
        let mut sh_holder: Option<SchemaHeader> = None;
        for sec in s.split(SECTION_HEADER_BOUNDARY_PATTERN) {
            if sh_holder.is_none() {
                // we are waiting on a valid header
                sh_holder = sec.parse::<SchemaHeader>().ok()
            } else {
                if let Some(sh) = sh_holder {
                    // we are waiting on a  body
                    match sh.object_type {
                        ObjectType::Table => schema.tables.push(SchemaSection {
                            header: sh,
                            body: String::from(sec),
                        }),
                        ObjectType::Type | ObjectType::Domain => schema.types.push(SchemaSection {
                            header: sh,
                            body: String::from(sec),
                        }),
                        ObjectType::FkConstraint | ObjectType::Constraint => {
                            schema.constraints.push(SchemaSection {
                                header: sh,
                                body: String::from(sec),
                            })
                        }
                        ObjectType::Index => schema.indexes.push(SchemaSection {
                            header: sh,
                            body: String::from(sec),
                        }),
                        ObjectType::Extension | ObjectType::Default => {
                            schema.general.push(SchemaSection {
                                header: sh,
                                body: String::from(sec),
                            })
                        }
                        ObjectType::Comment => schema.comments.push(SchemaSection {
                            header: sh,
                            body: String::from(sec),
                        }),
                        ObjectType::Function | ObjectType::Trigger => {
                            schema.functions.push(SchemaSection {
                                header: sh,
                                body: String::from(sec),
                            })
                        }
                        _ => (),
                    }
                    sh_holder = None;
                }
            }
        }
        Ok(schema)
    }
}

impl Schema {
    pub fn write_to_fs(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        for table in &self.tables {
            let section_path = path.join("tables").join(&table.header.schema);
            fs::create_dir_all(&section_path)?;
            let fp = section_path.join(format!("{}.sql", table.header.name));
            fs::write(fp, &table.body)?;
        }

        for function in &self.functions {
            let section_path = match function.header.name.contains("test_") {
                true => path
                    .join("tests")
                    .join("functions")
                    .join(&function.header.schema),
                false => path.join("functions").join(&function.header.schema),
            };
            fs::create_dir_all(&section_path)?;
            let fp = section_path.join(format!("{}.sql", function.header.name));
            fs::write(fp, &function.body)?;
        }

        for sql_type in &self.types {
            let section_path = path.join("types").join(&sql_type.header.schema);
            fs::create_dir_all(&section_path)?;
            let fp = section_path.join(format!("{}.sql", sql_type.header.name));
            fs::write(fp, &sql_type.body)?;
        }

        for constraint in &self.constraints {
            let table_name = constraint
                .body
                .trim_start_matches("\n")
                .strip_prefix("ALTER TABLE ONLY")
                .ok_or("Constraint format unknown")?
                .split("\n")
                .next()
                .ok_or("No newline found")?
                .split(".")
                .nth(1)
                .ok_or("Couldn't parse as schema.table")?
                .trim()
                .replace('"', "");
            let section_path = path.join("tables").join(&constraint.header.schema);
            fs::create_dir_all(&section_path)?;
            let fp = section_path.join(format!("{}.sql", table_name));
            let mut file = fs::OpenOptions::new().append(true).create(true).open(fp)?;

            writeln!(file, "{}", constraint.body)?;
        }

        for index in &self.indexes {
            let table_name = index
                .body
                .trim_start_matches('\n')
                .split_once(" ON ")
                .ok_or("no on clause")?
                .1
                .split_whitespace()
                .next()
                .ok_or("no table name")?
                .split('.')
                .nth(1)
                .ok_or("no table name after schema")?
                .trim()
                .replace('"', "");
            let section_path = path.join("tables").join(&index.header.schema);
            fs::create_dir_all(&section_path)?;
            let fp = section_path.join(format!("{}.sql", table_name));
            let mut file = fs::OpenOptions::new().append(true).create(true).open(fp)?;

            writeln!(file, "{}", index.body)?;
        }
        Ok(())
    }
}
