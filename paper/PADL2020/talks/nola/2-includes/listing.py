class SharpSat(Problem):
...
    def td_node_column_def(self,var):
        return td_node_column_def(var)
    def td_node_extra_columns(self):
        return [("model_count","NUMERIC")]
    def candidate_extra_cols(self,node):
        return ...
    def assignment_extra_cols(self,node):
        return ["sum(model_count) AS model_count"]
    def filter(self,node):
        return filter(self.var_clause_dict, node)
    def prepare_input(self, fname):
        input = CnfReader.from_file(fname)
        ...
        return cnf2primal(input.num_vars, input.clauses, self.var_clause_dict)
    def setup_extra(self):
        create_tables()
        insert_data()
    def after_solve(self):
        ...
