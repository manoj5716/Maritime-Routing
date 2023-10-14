-- FUNCTION: public.get_nearest_vertex_id(double precision, double precision)

-- DROP FUNCTION IF EXISTS public.get_nearest_vertex_id(double precision, double precision);

CREATE OR REPLACE FUNCTION public.get_nearest_vertex_id(
	lon double precision,
	lat double precision)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
   v_id integer;
begin
   SELECT id, ST_Distance(the_geom, concat('SRID=4326;POINT(',lon, ' ', lat, ')')::geometry) AS d 
   INTO v_id
   FROM maritime_routes_edges_vertices_pgr 
   ORDER BY d LIMIT 1;
   
   return v_id;
end;
$BODY$;

ALTER FUNCTION public.get_nearest_vertex_id(double precision, double precision)
    OWNER TO postgres;
