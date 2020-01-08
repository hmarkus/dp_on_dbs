%selection of buckets, 19 vertices
%b(19).
%source of projection variables, vertices 1, 2, ..., 100
%p(1..100).



B { abs(X) : p(X)} B :- b(B).

bucket(X,X) :- abs(X).
bucket(B,Z) :- bucket(B,Y), abs(Y), edge(Y,Z).
bucket(B,Z) :- bucket(B,Y), abs(Y), edge(Z,Y).
bucket(B,Z) :- bucket(B,Y), not abs(Y), edge(Y,Z).
bucket(B,Z) :- bucket(B,Y), not abs(Y), edge(Z,Y).

:~ X < Y, abs(Y), bucket(X,Y). [1,X,Y]













%:~ C = #count { X : bucket(B,X), not abs(X) }, bucket(B, _). [C,B]

%:~ M = #max { C : sum(_,C) }. [M]
%sum(B,C) :- C = #count { X : bucket(B,X) }, bucket(B, _).
%
%:~ X < Y, abstract(B,X), abstract(_,Y), bucket(B,Y), C = #count { Z : bucket(B,Z) }. [C,X,Y]


#show abs/1.
%#show sum/2.
%#show bucket/2.


%:~ M = #max { C: sum(_,C)}. [M]
%:~ C = #count { X : bucket(B,X) }, bucket(B,_). [C,B]

%aedge(X,Y) :- X < Y, abstract(B,X), abstract(_,Y), bucket(B,Y). %[1,X,Y]
%#show aedge/2.

%#show abstract/2.

%#show vertex/1.
%vertex(X) :- aedge(X,_).
%vertex(X) :- aedge(_,X).

