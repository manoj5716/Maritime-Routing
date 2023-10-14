-- DROP FUNCTION get_optimal_one_to_one_route(double precision, double precision, double precision, double precision);

create or replace function get_optimal_one_to_one_route (lon1 double precision, lat1 double precision, lon2 double precision, lat2 double precision) 
	returns table (
		id integer, 
		geom geometry(Geometry,4326)
	) 
	language plpgsql
as $$
declare
	v_start integer := get_nearest_vertex_id(lon1, lat1); 
	v_end integer := get_nearest_vertex_id(lon2, lat2);
begin
	RETURN QUERY
	SELECT * FROM get_optimal_one_to_one_route(v_start, v_end);
end;$$

