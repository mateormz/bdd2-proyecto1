import axios from "axios";

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export async function getHealth() {
	try {
		const response = await axios.get(`${API_BASE_URL}/health`);
		return response.data;
	} catch (error) {
		throw error;
	}
}