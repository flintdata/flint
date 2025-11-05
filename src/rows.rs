use crate::Value;

pub struct Rows {
    rows: Vec<Vec<Value>>,
    index: usize,
}

impl Rows {
    pub(crate) fn new(rows: Vec<Vec<Value>>) -> Self {
        Rows { rows, index: 0 }
    }

    pub fn next(&mut self) -> Option<Row<'_>> {
        if self.index < self.rows.len() {
            let row = Row {
                values: &self.rows[self.index],
            };
            self.index += 1;
            Some(row)
        } else {
            None
        }
    }
}

pub struct Row<'a> {
    values: &'a [Value],
}

impl<'a> Row<'a> {
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}