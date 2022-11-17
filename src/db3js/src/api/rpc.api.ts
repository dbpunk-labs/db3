import axios from "axios";

export interface ApiResponse<T> {
	status: "0" | "1";
	data: T;
}

const instance = axios.create({
	// baseURL: "http://localhost:26659",
	// baseURL: "https://v2.db3.network",
});

instance.interceptors.response.use(
	(response) => {
		console.log(response);
		const { result, error, id } = response.data;
		if (error) {
			console.error(`code：${error}`);
			return Promise.reject(error);
		}
		return { result, id };
	},
	(error) => {
		console.error(error);
		const errorMsg = error?.response?.data?.msg || error.toString();
		console.error(errorMsg);
		return Promise.reject(error);
	},
);

export default (method: string, params: any) => {
	return instance.post("/api", { jsonrpc: "2.0", id: 2, method, params });
};
