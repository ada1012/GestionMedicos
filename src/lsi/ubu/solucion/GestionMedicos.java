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
	
	public static void reservar_consulta(String m_NIF_cliente, 
			String m_NIF_medico,  Date m_Fecha_Consulta) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
		ResultSet rs = null;
		
		GestionMedicosException ges = null;

	
		try{
			con = pool.getConnection();
			
			st = con.prepareStatement("SELECT COUNT(*) AS num_consultas FROM consulta WHERE NIF_medico = ? AND Fecha_Consulta = ?");
            st.setString(1, m_NIF_medico);
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
            st = con.prepareStatement("INSERT INTO consulta (NIF_cliente, NIF_medico, Fecha_Consulta) VALUES (?, ?, ?)");
            st.setString(1, m_NIF_cliente);
            st.setString(2, m_NIF_medico);
            st.setDate(3, m_Fecha_Consulta);
            st.executeUpdate();
            st.close();
            
            // Incrementar el contador de consultas del médico en cuestión
            st = con.prepareStatement("UPDATE medico SET consultas = consultas + 1 WHERE NIF = ?");
            st.setString(1, m_NIF_medico);
            st.executeUpdate();
            st.close();
            
            con.commit(); // Confirma la transacción
			
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

		GestionMedicosException ges = null;
	
		try{
			con = pool.getConnection();
			
			//Verificar si existe una consulta para la Fecha de Consulta, Medico y Cliente en la tabla consulta
	        st = con.prepareStatement("SELECT * FROM consulta WHERE NIF_cliente = ? AND NIF_medico = ? AND Fecha_Consulta = ?");
	        st.setString(1, m_NIF_cliente);
	        st.setString(2, m_NIF_medico);
	        st.setDate(3, m_Fecha_Consulta);
	        rs = st.executeQuery();

	        if (!rs.next()) {
	        	ges = new GestionMedicosException(GestionMedicosException.CONSULTA_NO_EXISTE);
	        }
            rs.close();
            st.close();

	        //Verificar que la Fecha Anulacion debe ser como mínimo 2 días antes de la Fecha Consulta
	        Calendar calFechaConsulta = Calendar.getInstance();
	        calFechaConsulta.setTime(m_Fecha_Consulta);
	        Calendar calFechaAnulacion = Calendar.getInstance();
	        calFechaAnulacion.setTime(m_Fecha_Anulacion);
	        calFechaConsulta.add(Calendar.DATE, -2);
	        if (calFechaAnulacion.before(calFechaConsulta)) {
	        	ges = new GestionMedicosException(GestionMedicosException.CONSULTA_NO_ANULA);
	        }

	        //Actualizar la tabla medico restando 1 del número de consultas
	        st = con.prepareStatement("UPDATE medico SET consultas = consultas - 1 WHERE NIF_medico = ?");
	        st.setString(1, m_NIF_medico);
	        st.executeUpdate();

            st.close();

	        //Escribir un registro en la tabla anulacion
	        st = con.prepareStatement("INSERT INTO anulacion (NIF_cliente, NIF_medico, Fecha_Consulta, Fecha_Anulacion, motivo) VALUES (?, ?, ?, ?, ?)");
	        st.setString(1, m_NIF_cliente);
	        st.setString(2, m_NIF_medico);
	        st.setDate(3, m_Fecha_Consulta);
	        st.setDate(4, m_Fecha_Anulacion);
	        st.setString(5, motivo);
	        st.executeUpdate();
            
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
	}
	
	public static void consulta_medico(String m_NIF_medico)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		PreparedStatement st = null;
	    ResultSet rs = null;

	
		try{
			con = pool.getConnection();
			
			// Preparar la consulta SQL para obtener las consultas del médico correspondiente
	        String sql = "SELECT * FROM consulta WHERE NIF_medico = ? ORDER BY Fecha_Consulta";
	        st = con.prepareStatement(sql);
	        st.setString(1, m_NIF_medico);
	        
	        // Ejecutar la consulta SQL
	        rs = st.executeQuery();
	        
	        // Imprimir los resultados por la salida estándar
	        logger.info("Consultas del médico " + m_NIF_medico + ":");
	        while (rs.next()) {
	            Date fechaConsulta = rs.getDate("Fecha_Consulta");
	            String nifCliente = rs.getString("NIF_cliente");
	            System.out.println("- Fecha de consulta: " + fechaConsulta + ", NIF del cliente: " + nifCliente);
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
		
	}
}
