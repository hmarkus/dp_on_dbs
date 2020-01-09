%selection of buckets, 19 vertices
%b(19).
%source of projection variables, vertices 1, 2, ..., 100
%p(1..100).



B { abs(X) : p(X)} B :- b(B).

% abs is in its own bucket
bucket(X,X) :- abs(X).

%
% eat non-abs neighbors of abs bucket
bucket(X,Y) :- not abs(Y), abs(X), edge(Y,X).
bucket(X,Y) :- not abs(Y), abs(X), edge(X,Y).

% eat neighbors of eaten non-abs neighbors
bucket(B,Z) :- bucket(B,Y), not abs(Y), edge(Y,Z).
bucket(B,Z) :- bucket(B,Y), not abs(Y), edge(Z,Y).
%

% induced edges
:~ X < Y, abs(Y), bucket(X,Y). [1,X,Y]

% direct edges
:~ abs(X), abs(Y), edge(X,Y). [1,X,Y]












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

