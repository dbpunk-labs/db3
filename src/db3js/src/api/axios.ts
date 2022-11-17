import axios from "axios";
import { message } from "antd";

export interface ApiResponse<T> {
	status: "0" | "1";
	data: T;
}

const instance = axios.create({
	baseURL: "/api",
});

instance.interceptors.request.use((config) => {
	if (config.method === "get") {
		if (!config.params) {
			config.params = {};
		}
		// config.params._t = Date.now();
	}
	return config;
});
instance.interceptors.response.use(
	(response) => {
		const { result } = response.data;
		return result;
	},
	(error) => {
		console.error(error);
		const errorMsg = error?.response?.data?.msg || error.toString();
		message.error(errorMsg);
		return Promise.reject(error);
	},
);

export default instance;
