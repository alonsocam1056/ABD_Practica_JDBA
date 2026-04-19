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
		PreparedStatement stInsert = null;
		PreparedStatement stUpdate = null;
		String sentenciaInsert;
		String sentenciaUpdate;
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
	
		try{
			con = pool.getConnection();
			if(!existeHospital(con,m_ID_Hospital)) {
				con.rollback();
				throw new GestionDonacionesSangreException(3);
			}
			if(!existeDonante(con, m_NIF)) {
				con.rollback();
				throw new GestionDonacionesSangreException(1);
			}
			if(!comprobarCupo(con,m_NIF, m_Fecha_Donacion)) {
				con.rollback();
				throw new GestionDonacionesSangreException(4);
			}
			if(m_Cantidad < 0) {
				con.rollback();
				throw new GestionDonacionesSangreException(5);
			}
			if(m_Cantidad > 0.45) {
				con.rollback();
				throw new GestionDonacionesSangreException(5);
			}
			
			sentenciaInsert = "Insert into donacion values (seq_donacion.nextVal,?,?,?)";
			stInsert = con.prepareStatement(sentenciaInsert);
			stInsert.setString(1, m_NIF);
			stInsert.setFloat(2, m_Cantidad);
			stInsert.setDate(3, m_Fecha_Donacion);
			stInsert.executeUpdate();

			
			sentenciaUpdate = "Update reserva_hospital Set cantidad = cantidad + ? Where ID_Hospital = ?";
			stUpdate = con.prepareStatement(sentenciaUpdate);
			stUpdate.setFloat(1, m_Cantidad);
			stUpdate.setInt(2, m_ID_Hospital);
			stUpdate.executeUpdate();

			con.commit();
			
		} catch (SQLException e) {
			//Completar por el alumno			
			if (con != null) {
				con.rollback();
			}
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (stInsert != null) {
				stInsert.close();
			}
			if (stUpdate != null) {
				stUpdate.close();
			}
			if (con != null) {
				con.close();
			}
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
	private static boolean existeDonante(Connection con, String nif) throws SQLException {
	    PreparedStatement st = con.prepareStatement(
	        "SELECT 1 FROM donante WHERE NIF = ?");
	    st.setString(1, nif);
	    ResultSet rs = st.executeQuery();
	    boolean existe = rs.next();
	    rs.close();
	    st.close();
	    return existe;
	}
	private static boolean comprobarCupo(Connection con, String nifDonante, Date fecha) throws SQLException {
	    PreparedStatement st = con.prepareStatement(
	        "SELECT MAX(trunc(fecha_donacion)) AS ultima_fecha FROM donacion WHERE nif_donante = ?");
	    st.setString(1, nifDonante);
	    ResultSet rs = st.executeQuery();
	    Date ultimaFecha = null;
	    if (rs.next()) {
	        ultimaFecha = rs.getDate("ultima_fecha");
	    }
	    rs.close();
	    st.close();
	    // Si nunca ha donado → puedes decidir devolver true
	    if (ultimaFecha == null) return true;

	    long diferenciaMilisegundos = fecha.getTime() - ultimaFecha.getTime();
	    long dias = diferenciaMilisegundos / (1000 * 60 * 60 * 24);
	    boolean resultado = false;
	    if(dias >= 15) {
	    	resultado = true;
	    }
	    
	    return resultado;
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
			correr_tests_realizar_donacion();
			
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}			
		
	}
	static void correr_tests_realizar_donacion() {
	    test_realizar_donacion_ok();
	    test_realizar_donacion_hospital_no_existe();
	    test_realizar_donacion_donante_no_existe();
	    test_realizar_donacion_cupo_invalido();
	    test_realizar_donacion_cantidad_negativa();
	    test_realizar_donacion_cantidad_excesiva();
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
	static void test_realizar_donacion_ok() {

	    try {
	        reiniciar();

	        realizar_donacion("12345678A", 1, 0.30f, java.sql.Date.valueOf("2025-02-10"));

	        System.out.println("OK test_realizar_donacion_ok");

	    } catch (Exception e) {
	        logger.error("FALLO test_realizar_donacion_ok: " + e.getMessage());
	    }
	}
	static void test_realizar_donacion_hospital_no_existe() {

	    try {
	        reiniciar();

	        realizar_donacion("12345678A", 999, 0.30f, java.sql.Date.valueOf("2025-02-10"));

	        logger.error("FALLO test_realizar_donacion_hospital_no_existe: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 3) {
	            System.out.println("OK test_realizar_donacion_hospital_no_existe");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void test_realizar_donacion_donante_no_existe() {

	    try {
	        reiniciar();

	        realizar_donacion("00000000Z", 1, 0.30f, java.sql.Date.valueOf("2025-02-10"));

	        logger.error("FALLO test_realizar_donacion_donante_no_existe: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 1) {
	            System.out.println("OK test_realizar_donacion_donante_no_existe");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void test_realizar_donacion_cupo_invalido() {

	    try {
	        reiniciar();

	        // Última donación: 15/01/2025 → aquí ponemos 20/01/2025 (<15 días)
	        realizar_donacion("12345678A", 1, 0.30f, java.sql.Date.valueOf("2025-01-20"));

	        logger.error("FALLO test_realizar_donacion_cupo_invalido: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 4) {
	            System.out.println("OK test_realizar_donacion_cupo_invalido");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void test_realizar_donacion_cantidad_negativa() {

	    try {
	        reiniciar();

	        realizar_donacion("12345678A", 1, -0.10f, java.sql.Date.valueOf("2025-02-10"));

	        logger.error("FALLO test_realizar_donacion_cantidad_negativa: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 5) {
	            System.out.println("OK test_realizar_donacion_cantidad_negativa");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	static void test_realizar_donacion_cantidad_excesiva() {

	    try {
	        reiniciar();

	        realizar_donacion("12345678A", 1, 0.50f, java.sql.Date.valueOf("2025-02-10"));

	        logger.error("FALLO test_realizar_donacion_cantidad_excesiva: no lanzó excepción");

	    } catch (GestionDonacionesSangreException e) {

	        if (e.getErrorCode() == 5) {
	            System.out.println("OK test_realizar_donacion_cantidad_excesiva");
	        } else {
	            logger.error("FALLO código incorrecto: " + e.getErrorCode());
	        }

	    } catch (Exception e) {
	        logger.error("FALLO excepción inesperada: " + e.getMessage());
	    }
	}
	
}
