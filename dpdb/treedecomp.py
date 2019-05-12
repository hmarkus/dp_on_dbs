class TreeDecomp(object):
    root = None
    num_bags = 0
    tree_width = 0
    num_orig_vertices = 0
    edges = []

    def __init__(self, num_bags, tree_width, num_orig_vertices, root, bags, adj):
        self.num_bags = num_bags
        self.tree_width = tree_width
        self.num_orig_vertices = num_orig_vertices
        self.root = Node(root,bags[root])

        def add_node(parent,node):
            new_node = Node(node,bags[node])
            if parent:
                parent.add_child(new_node)
            else:
                self.root = new_node
            for n in adj[node]:
                if n not in visited:
                    self.edges.append((node,n))
                    visited.add(n)
                    add_node(new_node,n)
            
        visited = set([root])
        add_node(None,root)

    @property
    def nodes(self):
        return self.postorder()

    def postorder(self):
        r = []
        def postorder_node(node):
            for c in node.children:
                postorder_node(c)
            r.append(node)
        postorder_node(self.root)
        return r

class Node(object):
    id = 0
    vertices = {}
    parent = None
    children = []

    def __init__(self, id, vertices):
        self.id = id
        self.vertices = vertices
        self.parent = None
        self.children = []

    def __str__(self):
        return "{0}: {{{1}}}".format(self.id,", ".join(map(str,self.vertices)))

    def __repr__(self):
        return "<id: {0} vertices: {1} #children: {2}>".format(self.id, self.vertices, len(self.children))

    @property
    def stored_vertices(self):
        return [v for v in self.vertices if self.is_root() or v in self.parent.vertices]

    def add_child(self,child):
        self.children.append(child)
        child.parent = self

    def is_leaf(self):
        return self.children == []

    def is_root(self):
        return self.parent == None
