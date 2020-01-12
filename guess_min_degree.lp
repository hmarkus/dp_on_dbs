%selection of buckets, 19 vertices
%b(19).
%source of projection variables, vertices 1, 2, ..., 100
%p(1..100).


N { abs(X) : p(X) } N :- b(N).
:~ D = #count{ Y: edge(X,Y); Y: edge(Y,X) }, abs(X). [D,X]
#show abs/1.











%degree(X,D) :- D = #count{ Y: edge(X,Y); Y: edge(Y,X) }, abs(X).
%:~ degree(X,D). [D,X]

%#show degree/2.

