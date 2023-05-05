package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.sql.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.enunciado.GestionMedicosException;
import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;

public class GestionMedicos {
	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
	public static boolean comprueba_medico(String m_NIF_medico) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
		ResultSet rs = null;
		boolean retorno = false;
		
		GestionMedicosException ges = null;

	
		try{
			con = pool.getConnection();
			
			st = con.prepareStatement("SELECT COUNT(*) AS num_medicos FROM medico WHERE NIF = ?");
			st.setString(1, m_NIF_medico);
			rs = st.executeQuery();
			rs.next();
			int num_medicos = rs.getInt("num_medicos");
			if (num_medicos == 0) {
				ges = new GestionMedicosException(GestionMedicosException.MEDICO_NO_EXISTE);
				retorno = false;
			} else {
				retorno = true;
			}
			rs.close();
			st.close();
			
		} catch (SQLException e) {
			con.rollback();		
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (con!=null) con.close();
		}
		return retorno;
	}
	
public static boolean comprueba_cliente(String m_NIF_cliente) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
		ResultSet rs = null;
		boolean retorno = false;
		
		GestionMedicosException ges = null;

	
		try{
			con = pool.getConnection();
			
			st = con.prepareStatement("SELECT COUNT(*) AS num_clientes FROM cliente WHERE NIF = ?");
			st.setString(1, m_NIF_cliente);
			rs = st.executeQuery();
			rs.next();
			int num_clientes = rs.getInt("num_clientes");
			if (num_clientes == 0) {
				ges = new GestionMedicosException(GestionMedicosException.CLIENTE_NO_EXISTE);
				retorno = false;
			} else {
				retorno = true;
			}
			rs.close();
			st.close();
			
		} catch (SQLException e) {
			con.rollback();		
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (con!=null) con.close();
		}
		return retorno;
	}
	
	public static void reservar_consulta(String m_NIF_cliente, 
			String m_NIF_medico,  Date m_Fecha_Consulta) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
		ResultSet rs = null;
		int id_medico;
		
		GestionMedicosException ges = null;

	
		try{
			con = pool.getConnection();
			
			if (comprueba_medico(m_NIF_medico) && comprueba_cliente(m_NIF_cliente)) {
				st = con.prepareStatement("SELECT id_medico FROM medico WHERE NIF = ?");
				st.setString(1, m_NIF_medico);
				rs = st.executeQuery();
				rs.next();
				id_medico = rs.getInt("id_medico");
	            rs.close();
	            st.close();
				
				st = con.prepareStatement("SELECT COUNT(*) AS num_consultas FROM consulta WHERE id_medico = ? AND fecha_consulta = ?");
	            st.setInt(1, id_medico);
	            st.setDate(2, m_Fecha_Consulta);
	            rs = st.executeQuery();
	            rs.next();
	            int num_consultas = rs.getInt("num_consultas");
	            if (num_consultas > 0) {
	            	ges = new GestionMedicosException(GestionMedicosException.MEDICO_OCUPADO);
	            }
	            rs.close();
	            st.close();
	            
	            // Insertar la nueva consulta
	            st = con.prepareStatement("INSERT INTO consulta VALUES (SEQ_CONSULTA.NEXTVAL, ?, ?, ?)");

	            st.setDate(1, m_Fecha_Consulta);
	            st.setInt(2, id_medico);
	            st.setString(3, m_NIF_cliente);
	            st.executeUpdate();
	            st.close();
	            
	            // Incrementar el contador de consultas del médico en cuestión
	            st = con.prepareStatement("UPDATE medico SET consultas = consultas + 1 WHERE NIF = ?");
	            st.setString(1, m_NIF_medico);
	            st.executeUpdate();
	            st.close();
	            
	            con.commit(); // Confirma la transacción
	            
	            logger.info("Consulta reservada");
			}
			
		} catch (SQLException e) {
			con.rollback();		
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (con!=null) con.close();
		}
		
		
	}
	
	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico,  
			Date m_Fecha_Consulta, Date m_Fecha_Anulacion, String motivo)
			throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
		ResultSet rs = null;
		int id_medico;

		GestionMedicosException ges = null;
	
		try{
			con = pool.getConnection();
			
			if (comprueba_medico(m_NIF_medico) && comprueba_cliente(m_NIF_cliente)) {
				st = con.prepareStatement("SELECT id_medico FROM medico WHERE NIF = ?");
				st.setString(1, m_NIF_medico);
				rs = st.executeQuery();
				rs.next();
				id_medico = rs.getInt("id_medico");
	            rs.close();
	            st.close();
	            
				//Verificar si existe una consulta para la Fecha de Consulta, Medico y Cliente en la tabla consulta
		        st = con.prepareStatement("SELECT * FROM consulta WHERE NIF = ? AND id_medico = ? AND fecha_consulta = ?");
		        st.setString(1, m_NIF_cliente);
		        st.setInt(2, id_medico);
		        st.setDate(3, m_Fecha_Consulta);
		        rs = st.executeQuery();
		        int id_consulta = 0;
	
		        if (!rs.next()) {
		        	ges = new GestionMedicosException(GestionMedicosException.CONSULTA_NO_EXISTE);
		        } else {
		        	id_consulta = rs.getInt("id_consulta");
		        
		            rs.close();
		            st.close();
		
			        //Verificar que la Fecha Anulacion debe ser como mínimo 2 días antes de la Fecha Consulta
			        Calendar calFechaConsulta = Calendar.getInstance();
			        calFechaConsulta.setTime(m_Fecha_Consulta);
			        Calendar calFechaAnulacion = Calendar.getInstance();
			        calFechaAnulacion.setTime(m_Fecha_Anulacion);
			        calFechaConsulta.add(Calendar.DATE, -2);
			        if (!calFechaAnulacion.before(calFechaConsulta)) {
			        	ges = new GestionMedicosException(GestionMedicosException.CONSULTA_NO_ANULA);
			        } else {
				        //Actualizar la tabla medico restando 1 del número de consultas
				        st = con.prepareStatement("UPDATE medico SET consultas = consultas - 1 WHERE NIF = ?");
				        st.setString(1, m_NIF_medico);
				        st.executeUpdate();
			
			            st.close();
			            
				        //Escribir un registro en la tabla anulacion
				        st = con.prepareStatement("INSERT INTO anulacion VALUES (SEQ_ANULACION.NEXTVAL, ?, ?, ?)");
				        st.setInt(1, id_consulta);
				        st.setDate(2, m_Fecha_Anulacion);
				        st.setString(3, motivo);
				        st.executeUpdate();
			            
			            st.close();
			        }
		        }
	            
	            con.commit();
	            logger.info("Consulta anulada");
			}
			
		} catch (SQLException e) {
			con.rollback();
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (con!=null) con.close();
		}		
	}
	
	public static void consulta_medico(String m_NIF_medico)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
	    ResultSet rs = null;
	    int id_medico;
	
		try{
			con = pool.getConnection();
			
			if (comprueba_medico(m_NIF_medico)) {
				st = con.prepareStatement("SELECT id_medico FROM medico WHERE NIF = ?");
				st.setString(1, m_NIF_medico);
				rs = st.executeQuery();
				rs.next();
				id_medico = rs.getInt("id_medico");
	            rs.close();
	            st.close();
	            
				// Preparar la consulta SQL para obtener las consultas del médico correspondiente
		        String sql = "SELECT * FROM consulta WHERE id_medico = ? ORDER BY fecha_consulta";
		        st = con.prepareStatement(sql);
		        st.setInt(1, id_medico);
		        
		        // Ejecutar la consulta SQL
		        rs = st.executeQuery();
		        
		        // Imprimir los resultados por la salida estándar
		        logger.info("Consultas del médico " + m_NIF_medico + ":");
		        while (rs.next()) {
		            Date fechaConsulta = rs.getDate("fecha_consulta");
		            String nifCliente = rs.getString("NIF");
		            System.out.println("- Fecha de consulta: " + fechaConsulta + ", NIF del cliente: " + nifCliente);
		        }
	
	            con.commit();
			}
		} catch (SQLException e) {
			con.rollback();			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (rs!=null) rs.close();
			if (st!=null) st.close();
			if (con!=null) con.close();
		}		
	}
	
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
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
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}

	    try {
		    // Test 1: Reservar una consulta válida y luego anularla.
	    	System.out.println("Test 1");
	        reservar_consulta("12345678A", "222222B", Date.valueOf("2023-05-06"));
	        anular_consulta("12345678A", "222222B", Date.valueOf("2023-05-06"), Date.valueOf("2023-05-03"), "No me siento bien");
	        consulta_medico("222222B");

	        // Test 2: Intentar reservar una consulta para un médico que ya tiene una consulta reservada en esa fecha.
	    	System.out.println("");
	    	System.out.println("Test 2");
	        reservar_consulta("12345678A", "8766788Y", Date.valueOf("2023-05-06"));
	        reservar_consulta("87654321B", "8766788Y", Date.valueOf("2023-05-06")); // Este intento debe lanzar una SQLException
	    
	        // Test 3: Intentar anular una consulta con una fecha de anulación que no cumple con el requisito mínimo de dos días.
	    	System.out.println("");
	    	System.out.println("Test 3");
	        reservar_consulta("12345678A", "8766788Y", Date.valueOf("2023-05-09"));
	        anular_consulta("12345678A", "8766788Y", Date.valueOf("2023-05-09"), Date.valueOf("2023-05-08"), "No me siento bien"); // Este intento debe lanzar una SQLException

	        // Test 4: Intentar anular una consulta que no existe.
	    	System.out.println("");
	    	System.out.println("Test 4");
	        anular_consulta("12345678A", "8766788Y", Date.valueOf("2023-05-25"), Date.valueOf("2023-05-10"), "No me siento bien"); // Este intento debe lanzar una SQLException

	        // Test 5: Intentar consultar un médico que no tiene consultas reservadas.
	    	System.out.println("");
	    	System.out.println("Test 5");
	        consulta_medico("87654321A"); // Este intento debe lanzar una SQLException

	        // Test 6: Intentar reservar una colsulta para un médico que está ocupado ese día.
	    	System.out.println("");
	    	System.out.println("Test 6");
	        reservar_consulta("12345678A", "8766788Y", Date.valueOf("2023-05-16"));
	        reservar_consulta("87654321B", "8766788Y", Date.valueOf("2023-05-16")); // Este intento debe lanzar una SQLException
	    } catch (SQLException e) {
	        logger.error(e.getMessage());
	    }		
		
	}
}
