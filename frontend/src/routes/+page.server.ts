import { redirect, type Actions } from '@sveltejs/kit';

export const actions = {
	default: async ({ request }) => {
		// [TODO] actually call backend, start streaming data
		const data = await request.formData();

		// redirect to loading page while data is fetched
		redirect(303, '/users');
		return { success: true };
	}
} satisfies Actions;
