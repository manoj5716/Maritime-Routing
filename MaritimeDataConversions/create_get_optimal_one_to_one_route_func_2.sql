-- DROP FUNCTION get_optimal_one_to_one_route(integer,integer);

create or replace function get_optimal_one_to_one_route (
  v_start integer,
  v_end integer
) 
	returns table (
		id integer, 
		geom geometry(Geometry,4326)
	) 
	language plpgsql
as $$
begin
	RETURN QUERY 
	SELECT pt.id, pt.geom 
	FROM pgr_Dijkstra(
	  'select id, source, target, cost, reverse_cost from maritime_routes_edges',
	  v_start, v_end, false) AS di 
	JOIN maritime_routes_edges AS pt
	ON di.edge = pt.id;
end;$$
