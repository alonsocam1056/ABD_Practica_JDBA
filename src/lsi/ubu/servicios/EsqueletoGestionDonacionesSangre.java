package lsi.ubu.servicios;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;


/**
 * GestionDonacionesSangre:
 * Implementa la gestion de donaciones de sangre según el enunciado del ejercicio
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @author <a href="mailto:srarribas@ubu.es">Sandra Rodriguez</a>
 * @version 1.5
 * @since 1.0 
 */
public class EsqueletoGestionDonacionesSangre {
	
	private static Logger logger = LoggerFactory.getLogger(EsqueletoGestionDonacionesSangre.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
	public static void realizar_donacion(String m_NIF, int m_ID_Hospital,
			float m_Cantidad,  Date m_Fecha_Donacion) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			//Completar por el alumno
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno*/
		}
		
		
	}
	
	public static void anular_traspaso(int m_ID_Tipo_Sangre, int m_ID_Hospital_Origen,int m_ID_Hospital_Destino,
			Date m_Fecha_Traspaso)
			throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		String sentenciaSelect = "";
		String sentenciaUpdate = "";
		String sentenciaDelete = "";
		PreparedStatement stSelect = null;
		PreparedStatement stUpdate = null;
		PreparedStatement stDelete = null;
		ResultSet rs = null;

	
		try{
			con = pool.getConnection();
			sentenciaSelect ="SELECT cantidad " +
		            "FROM traspaso " +
		            "WHERE id_tipo_sangre = ? " +
		            "AND id_hospital_origen = ? " +
		            "AND id_hospital_destino = ? " +
		            "AND fecha_traspaso = trunc(?)";
			sentenciaUpdate = "UPDATE reserva_hospital " +
		            "SET cantidad = cantidad + ? " +
		            "WHERE id_tipo_sangre = ? AND id_hospital = ?";
			sentenciaDelete = "DELETE FROM traspaso " +
		            "WHERE id_tipo_sangre = ? " +
		            "AND id_hospital_origen = ? " +
		            "AND id_hospital_destino = ? " +
		            "AND fecha_traspaso = trunc(?)";
			stSelect = con.prepareStatement(sentenciaSelect);
			stSelect.setInt(1, m_ID_Tipo_Sangre);
			stSelect.setInt(2, m_ID_Hospital_Origen);
			stSelect.setInt(3, m_ID_Hospital_Destino);
			stSelect.setDate(4, m_Fecha_Traspaso);
			rs = stSelect.executeQuery();
			if (rs.next()) {
				float cantidad = rs.getFloat("cantidad");
				
				stUpdate = con.prepareStatement(sentenciaUpdate);
				stUpdate.setFloat(1, cantidad);
				stUpdate.setInt(2, m_ID_Tipo_Sangre);
				stUpdate.setInt(3, m_ID_Hospital_Origen);
				stUpdate.executeUpdate();
				
				stUpdate.setFloat(1, -cantidad);
				stUpdate.setInt(3, m_ID_Hospital_Destino);
				stUpdate.executeUpdate();
				
				stDelete = con.prepareStatement(sentenciaDelete);
				stDelete.setInt(1, m_ID_Tipo_Sangre);
				stDelete.setInt(2, m_ID_Hospital_Origen);
				stDelete.setInt(3, m_ID_Hospital_Destino);
				stDelete.setDate(4, m_Fecha_Traspaso);
				stDelete.executeUpdate();
				con.commit();	
            }
			
			
			else {
				con.rollback();
				if (!existeTipoSangre(con, m_ID_Tipo_Sangre)) {
					throw new GestionDonacionesSangreException(2);
				}
				if (!existeTipoSangre(con, m_ID_Hospital_Origen) || !existeTipoSangre(con, m_ID_Hospital_Destino)) {
					throw new GestionDonacionesSangreException(3);
				}
				
			}
			
			
			//Completar por el alumno
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			if (con != null) {
				con.rollback();
			}
			//if (e.getErrorCode() >= 1 && e.getErrorCode() <= 7) {
				//throw new GestionDonacionesSangreException(e.getErrorCode());
			//}
			logger.error(e.getMessage());
			throw e;	

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (stSelect != null) {
				stSelect.close();
			}
			if (stDelete != null) {
				stDelete.close();
			}
			if (stUpdate != null) {
				stUpdate.close();
			}
			if (con != null) {
				con.close();
			}
			/*A rellenar por el alumno*/
		}		
	}
	private static boolean existeTipoSangre(Connection con, int id) throws SQLException {
	    PreparedStatement st = con.prepareStatement(
	        "SELECT 1 FROM tipo_sangre WHERE id_tipo_sangre = ?");
	    st.setInt(1, id);
	    ResultSet rs = st.executeQuery();
	    boolean existe = rs.next();
	    rs.close();
	    st.close();
	    return existe;
	}
	private static boolean existeHospital(Connection con, int id) throws SQLException {
	    PreparedStatement st = con.prepareStatement(
	        "SELECT 1 FROM hospital WHERE id_hospital = ?");
	    st.setInt(1, id);
	    ResultSet rs = st.executeQuery();
	    boolean existe = rs.next();
	    rs.close();
	    st.close();
	    return existe;
	}

	

	
	public static void consulta_traspasos(String m_Tipo_Sangre)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			//Completar por el alumno
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno*/
		}		
	}
	
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_donaciones_sangre.sql");
	}

	static void tests() throws SQLException{
		creaTablas();
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();		
		
		//Relatar caso por caso utilizando el siguiente procedure para inicializar los datos
		
		CallableStatement cll_reinicia=null;
		Connection conn = null;
		
		try {
			//Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			correr_tests_anular_traspaso();
			
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}			
		
	}
	static void test_anular_traspaso_ok() {

	    try {
	        reiniciar();

	        anular_traspaso(1, 1, 2, java.sql.Date.valueOf("2025-01-11"));

	        System.out.println("OK test_anular_traspaso_ok");

	    } catch (Exception e) {
	        logger.error("FALLO test_anular_traspaso_ok: " + e.getMessage());
	    }
	}
	static void test_anular_traspaso_tipo_sangre_no_existe() {

	    try {
	        reiniciar();

	        anular_traspaso(33, 1, 2, java.sql.Date.valueOf("2026-01-01"));

	        logger.error("FALLO test_anular_traspaso_hospital_inexistente: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 2) {
	        	System.out.println("OK test_anular_traspaso_tipo_sangre_no_existe");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }
	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void test_anular_traspaso_hospital_no_existe() {

	    try {
	        reiniciar();

	        anular_traspaso(1, 33, 33, java.sql.Date.valueOf("2026-01-01"));

	        logger.error("FALLO test_anular_traspaso_hospital_no_existe: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 3) {
	        	System.out.println("OK test_anular_traspaso_hospital_no_existe");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	    	logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void reiniciar() throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		
		CallableStatement cll_reinicia=null;
		Connection conn = null;
		
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}	
	}
	static void correr_tests_anular_traspaso() {
		test_anular_traspaso_ok();
		test_anular_traspaso_tipo_sangre_no_existe();
		test_anular_traspaso_hospital_no_existe();
	}
}
