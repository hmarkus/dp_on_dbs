class TreeDecomp(object):
    root = None
    edges = []
    leafs = []

    def __init__(self, num_bags, tree_width, num_orig_vertices, root, bags, adj, minor_graph):
        self.num_bags = num_bags
        self.tree_width = tree_width
        self.num_orig_vertices = num_orig_vertices
        self.root = None
        self.mg = minor_graph
        self.edges = []
        self.leafs = []

        # iterative because we can hit stack limit for large trees if recursive
        def add_nodes(root):
            worklist = [(root,None)]
            for n in worklist:
                node = n[0]
                parent = n[1]
                new_node = Node(node,bags[node], set() if self.mg is None else self.mg.projectionVariablesOf(bags[node]))
                if parent:
                    parent.add_child(new_node)
                else:
                    self.root = new_node
                leaf = True
                if adj:
                    for a in adj[node]:
                        if a not in visited:
                            self.edges.append((node,a))
                            visited.add(a)
                            worklist.append((a,new_node))
                            leaf = False
                if leaf:
                    self.leafs.append(new_node)
            
        visited = set([root])
        add_nodes(root)

    @property
    def nodes(self):
        return self.postorder()

    def postorder(self):
        r = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            for c in node.children:
                stack.append(c)
            r.insert(0,node)
        return r

class Node(object):
    def __init__(self, id, vertices, minor_vertices=None):
        self.id = id
        self.vertices = vertices
        self.minor_vertices = minor_vertices
        self.all_vertices = set(minor_vertices)
        self.all_vertices.update(vertices)
        self.parent = None
        self.children = []
        self._vertex_child_map = {v: [] for v in vertices}
        self.constraint_relevant = []

    def __str__(self):
        return "{0}: {{{1}}}".format(self.id,", ".join(map(str,self.vertices)))

    def __repr__(self):
        return "<id: {0} vertices: {1} #children: {2}>".format(self.id, self.vertices, len(self.children))

    @property
    def stored_vertices(self):
        return [v for v in self.vertices if self.is_root() or v in self.parent.vertices]

    @property
    def edges(self):
        return self.children + [self.parent]

    def is_minor(self, vertex):
        return vertex in self.minor_vertices

    def needs_introduce(self, vertex):
        return not vertex in self._vertex_child_map or self._vertex_child_map[vertex] == []

    def vertex_children(self,vertex):
        return self._vertex_child_map[vertex]

    def add_vertices(self, vertices):
        for v in vertices:
            if v not in self._vertex_child_map:
                self._vertex_child_map[v] = []
            if v not in self.vertices:
                self.vertices.append(v)
                for c in self.children:
                    if v in c.vertices:
                        self._vertex_child_map[v].append(c)

    def add_child(self, child):
        self.children.append(child)
        child.parent = self
        for v in self.vertices:
            if v in child.vertices:
                self._vertex_child_map[v].append(child)

    def is_leaf(self):
        return self.children == []

    def is_root(self):
        return self.parent == None
